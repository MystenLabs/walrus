// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Test utilities for `walrus-sui`.

// Allowing large futures in test utils.
#![allow(clippy::large_futures)]

pub mod system_setup;

#[cfg(not(msim))]
use std::sync::mpsc;
use std::{
    collections::{BTreeSet, HashMap},
    fmt::{self, Debug, Formatter},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

use anyhow::anyhow;
use serde::Serialize;
#[cfg(msim)]
use sui_config::local_ip_utils;
use sui_sdk::sui_client_config::SuiEnv;
#[cfg(msim)]
use sui_simulator::runtime::NodeHandle;
use sui_types::{
    base_types::{ObjectID, SuiAddress},
    crypto::ToFromBytes,
    digests::TransactionDigest,
    event::EventID,
    programmable_transaction_builder::ProgrammableTransactionBuilder,
    transaction::TransactionData,
};
use test_cluster::{FullNodeHandle, TestCluster, TestClusterBuilder};
#[cfg(not(msim))]
use tokio::runtime::Runtime;
use tokio::sync::Mutex;
#[cfg(msim)]
use tokio::sync::mpsc;
use walrus_core::{
    BlobId,
    DEFAULT_ENCODING,
    Epoch,
    keys::{NetworkKeyPair, ProtocolKeyPair},
    messages::{
        BlobPersistenceType,
        Confirmation,
        ConfirmationCertificate,
        InvalidBlobCertificate,
        InvalidBlobIdMsg,
        ProtocolMessage,
        ProtocolMessageCertificate,
    },
};
use walrus_test_utils::WithTempDir;

use crate::{
    client::SuiContractClient,
    config::load_wallet_context_from_path,
    types::{
        BlobCertified,
        BlobDeleted,
        BlobRegistered,
        Committee,
        InvalidBlobId,
        NetworkAddress,
        StorageNode,
    },
    utils::create_wallet,
    wallet::Wallet,
};

/// Default gas budget for some transactions in tests and benchmarks.
const DEFAULT_GAS_BUDGET: u64 = 500_000_000;
/// Default amount of coin sent when funding a wallet.
pub const DEFAULT_FUNDING_PER_COIN: u64 = 1_000_000_000_000;
/// A fixed `ObjectID` used by default when creating events for testing.
pub const FIXED_OBJECT_ID: ObjectID = ObjectID::from_single_byte(42);

/// Returns a random `EventID` for testing.
pub fn event_id_for_testing() -> EventID {
    EventID {
        tx_digest: TransactionDigest::random(),
        event_seq: 0,
    }
}

/// Returns an arbitrary (fixed) `EventID` for testing with a variable sequence number.
pub fn fixed_event_id_for_testing(event_seq: u64) -> EventID {
    EventID {
        tx_digest: TransactionDigest::new([42; 32]),
        event_seq,
    }
}

/// Returns an arbitrary (fixed) `ObjectID` for testing.
pub fn object_id_for_testing() -> ObjectID {
    ObjectID::from_single_byte(42)
}

/// Represents a test cluster running within this process or as a separate process.
// Allowing a large enum variant as this is anyway just used in tests.
#[allow(clippy::large_enum_variant)]
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
    additional_fullnodes: Vec<FullNodeHandle>,

    #[cfg(msim)]
    node_handle: NodeHandle,
}

impl Debug for TestClusterHandle {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("TestClusterHandle").finish()
    }
}

#[cfg(not(msim))]
impl TestClusterHandle {
    // Creates a test Sui cluster using tokio runtime.
    fn new(runtime: &Runtime) -> Self {
        Self::from_env().unwrap_or_else(|| Self::new_on_runtime(runtime, None))
    }

    // Creates a test Sui cluster using tokio runtime.
    #[cfg(not(msim))]
    fn new_on_runtime(runtime: &Runtime, num_additional_fullnodes: Option<usize>) -> Self {
        tracing::debug!("building global Sui test cluster");
        let (tx, rx) = mpsc::channel();
        runtime.spawn(async move {
            let mut test_cluster = sui_test_cluster().await;
            let wallet_path = test_cluster.wallet().config.path().to_path_buf();

            let mut full_node_handles = vec![];
            if let Some(num_additional_fullnodes) = num_additional_fullnodes {
                for _ in 0..num_additional_fullnodes {
                    let full_node_handle = test_cluster.spawn_new_fullnode().await;
                    full_node_handles.push(full_node_handle);
                }
            }

            tx.send((test_cluster, wallet_path, full_node_handles))
                .expect("can send test cluster");
        });

        let (cluster, wallet_path, full_node_handles) =
            rx.recv().expect("should receive test_cluster");

        Self {
            wallet_path: Mutex::new(wallet_path),
            cluster: LocalOrExternalTestCluster::Local { cluster },
            additional_fullnodes: full_node_handles,
        }
    }

    /// Attempts to construct a handle to an externally running sui cluster.
    ///
    /// If the environment variable `SUI_TEST_CONFIG_DIR` is defined, then the wallet and network
    /// configuration information taken from the the associated Sui files in the specified
    /// directory.
    ///
    /// Returns None if the environment variable is not set.
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
            additional_fullnodes: Vec::new(),
        })
    }

    /// Returns the test cluster reference.
    pub fn cluster(&self) -> &LocalOrExternalTestCluster {
        &self.cluster
    }

    /// Funds the provided addresses with `amount` Sui, or with a default amount
    /// if no amount is provided.
    pub async fn fund_addresses_with_sui(
        &self,
        addresses: Vec<SuiAddress>,
        amount: Option<u64>,
    ) -> anyhow::Result<()> {
        let path_guard = self.wallet_path.lock().await;
        // Load the cluster's wallet from file instead of using the wallet stored in the cluster.
        // This prevents tasks from being spawned in the current runtime that are expected by
        // the wallet to continue running.
        let mut cluster_wallet = load_wallet_context_from_path(Some(path_guard.as_path()), None)?;

        fund_addresses(&mut cluster_wallet, addresses, amount).await?;

        drop(path_guard);
        Ok(())
    }
}

#[cfg(msim)]
impl TestClusterHandle {
    // Creates a test Sui cluster using deterministic MSIM runtime.
    async fn new() -> Self {
        Self::new_with_additional_fullnodes(None).await
    }

    /// Creates a test Sui cluster using deterministic MSIM runtime
    /// with the specified number of fullnodes.
    #[cfg(msim)]
    async fn new_with_additional_fullnodes(num_additional_fullnodes: Option<usize>) -> Self {
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
                    let mut full_node_handles = vec![];
                    if let Some(num_additional_fullnodes) = num_additional_fullnodes {
                        for _ in 0..num_additional_fullnodes {
                            let full_node_handle = test_cluster.spawn_new_fullnode().await;
                            full_node_handles.push(full_node_handle);
                        }
                    }
                    tx.send((test_cluster, wallet_path, full_node_handles))
                        .await
                        .expect("Notifying cluster creation must succeed");
                }
            })
            .build();
        let Some((cluster, wallet_path, full_node_handles)) = rx.recv().await else {
            panic!("Unexpected end of channel");
        };
        Self {
            wallet_path: Mutex::new(wallet_path),
            cluster: LocalOrExternalTestCluster::Local { cluster },
            node_handle,
            additional_fullnodes: full_node_handles,
        }
    }

    /// Returns the local test cluster reference for simtests.
    pub fn cluster(&self) -> &TestCluster {
        let LocalOrExternalTestCluster::Local { ref cluster } = self.cluster else {
            unreachable!("always use a local test cluster in simtests")
        };
        cluster
    }

    /// Returns the simulator node handle for the Sui test cluster.
    pub fn sim_node_handle(&self) -> &NodeHandle {
        &self.node_handle
    }
}

impl TestClusterHandle {
    /// Returns the path to the wallet config file.
    pub async fn wallet_path(&self) -> PathBuf {
        self.wallet_path.lock().await.clone()
    }

    /// Returns the URL of the RPC node.
    pub fn rpc_url(&self) -> String {
        self.cluster.rpc_url()
    }

    /// Returns the additional fullnodes.
    pub fn additional_rpc_urls(&self) -> Vec<String> {
        self.additional_fullnodes
            .iter()
            .map(|node| node.rpc_url.clone())
            .collect()
    }

    /// Returns the additional fullnodes.
    pub fn additional_fullnodes(&self) -> &[FullNodeHandle] {
        &self.additional_fullnodes
    }
}

/// Handler for the global Sui test cluster using the tokio runtime.
#[cfg(not(msim))]
pub mod using_tokio {
    use std::{
        sync::{Arc, OnceLock, Weak},
        thread,
    };

    use tokio::{
        runtime::{Builder, Runtime},
        sync::Mutex as TokioMutex,
    };

    use super::TestClusterHandle;

    struct GlobalTestClusterHandler {
        inner: Weak<TokioMutex<TestClusterHandle>>,
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

        fn get_test_cluster_handle(&mut self) -> Arc<TokioMutex<TestClusterHandle>> {
            if let Some(handle) = self.inner.upgrade() {
                handle
            } else {
                let handle = Arc::new(tokio::sync::Mutex::new(TestClusterHandle::new(
                    &self.runtime,
                )));
                self.inner = Arc::downgrade(&handle);
                handle
            }
        }
    }

    /// Returns a handle to the global instance of a Sui test cluster and the wallet config path.
    ///
    /// Initializes the test cluster if it doesn't exist yet.
    ///
    /// Wrap the test cluster handle in an `Arc<Mutex<>>` so that the test body can change the
    /// Sui cluster internals.
    pub fn global_sui_test_cluster() -> Arc<TokioMutex<TestClusterHandle>> {
        static CLUSTER: OnceLock<std::sync::Mutex<GlobalTestClusterHandler>> = OnceLock::new();
        CLUSTER
            .get_or_init(|| std::sync::Mutex::new(GlobalTestClusterHandler::new()))
            .lock()
            .expect("mutex should not be poisoned")
            .get_test_cluster_handle()
    }
}

/// Creates a wallet for testing in a temporary directory.
pub async fn temp_dir_wallet(
    request_timeout: Option<Duration>,
    env: SuiEnv,
) -> anyhow::Result<WithTempDir<Wallet>> {
    let temp_dir = tempfile::tempdir().expect("temporary directory creation must succeed");
    let wallet = create_wallet(
        &temp_dir.path().join("wallet_config.yaml"),
        env,
        None,
        request_timeout,
    )
    .await?;

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
    ///
    /// Wrap the test cluster handle in an `Arc<Mutex<>>` so that the test body can change the
    /// Sui cluster internals.
    pub async fn global_sui_test_cluster() -> Arc<tokio::sync::Mutex<TestClusterHandle>> {
        Arc::new(tokio::sync::Mutex::new(TestClusterHandle::new().await))
    }

    /// Returns a handle to a newly created global instance of a Sui test cluster with the specified
    /// number of fullnodes.
    pub async fn global_sui_test_cluster_with_additional_fullnodes(
        num_additional_fullnodes: Option<usize>,
    ) -> Arc<tokio::sync::Mutex<TestClusterHandle>> {
        Arc::new(tokio::sync::Mutex::new(
            TestClusterHandle::new_with_additional_fullnodes(num_additional_fullnodes).await,
        ))
    }
}

/// Creates `n_wallets` wallets and funds them with the cluster's initial wallet.
///
/// Funds all the wallets with a single transaction, so as to avoid contention on the cluster's
/// wallet.
///
/// See [`new_wallet_on_sui_test_cluster`] for a similar method that funds a single wallet at a
/// time.
pub async fn create_and_fund_wallets_on_cluster(
    sui_cluster: Arc<tokio::sync::Mutex<TestClusterHandle>>,
    n_wallets: usize,
) -> anyhow::Result<Vec<WithTempDir<Wallet>>> {
    let sui_cluster = sui_cluster.lock().await;
    let path_guard = sui_cluster.wallet_path.lock().await;
    // Load the cluster's wallet from file instead of using the wallet stored in the cluster.
    // This prevents tasks from being spawned in the current runtime that are expected by
    // the wallet to continue running.
    let mut cluster_wallet = load_wallet_context_from_path(Some(path_guard.as_path()), None)?;

    let mut wallets = vec![];
    let mut addresses = vec![];
    for _ in 0..n_wallets {
        let mut wallet = wallet_for_testing(&mut cluster_wallet, false).await?;
        addresses.push(
            wallet
                .inner
                .active_address()
                .expect("newly created wallet has an active address"),
        );
        wallets.push(wallet);
    }

    fund_addresses(&mut cluster_wallet, addresses, None).await?;

    drop(path_guard);
    Ok(wallets)
}

/// Returns a new wallet on the global Sui test cluster.
pub async fn new_wallet_on_sui_test_cluster(
    sui_cluster: Arc<tokio::sync::Mutex<TestClusterHandle>>,
) -> anyhow::Result<WithTempDir<Wallet>> {
    let sui_cluster = sui_cluster.lock().await;
    let path_guard = sui_cluster.wallet_path.lock().await;
    // Load the cluster's wallet from file instead of using the wallet stored in the cluster.
    // This prevents tasks from being spawned in the current runtime that are expected by
    // the wallet to continue running.
    let mut cluster_wallet = load_wallet_context_from_path(Some(path_guard.as_path()), None)?;
    let wallet = wallet_for_testing(&mut cluster_wallet, true).await?;
    drop(path_guard);
    Ok(wallet)
}

/// Returns a new `SuiContractClient` on the global Sui test cluster.
pub async fn new_contract_client_on_sui_test_cluster(
    sui_cluster_handle: Arc<tokio::sync::Mutex<TestClusterHandle>>,
    existing_client: &SuiContractClient,
) -> anyhow::Result<WithTempDir<SuiContractClient>> {
    let contract_config = existing_client.read_client().contract_config();
    let walrus_client = new_wallet_on_sui_test_cluster(sui_cluster_handle)
        .await?
        .and_then_async(async |wallet| {
            let rpc_urls = &[wallet.get_rpc_url()?];
            SuiContractClient::new(
                wallet,
                rpc_urls,
                &contract_config,
                existing_client.read_client().backoff_config().clone(),
                None,
            )
            .await
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
    funding_wallet: &mut Wallet,
    funded: bool,
) -> anyhow::Result<WithTempDir<Wallet>> {
    let temp_dir = tempfile::tempdir().expect("temporary directory creation must succeed");

    let mut wallet = create_wallet(
        &temp_dir.path().join("wallet_config.yaml"),
        funding_wallet.get_active_env()?.to_owned(),
        None,
        None,
    )
    .await?;

    if funded {
        fund_addresses(funding_wallet, vec![wallet.active_address()?], None).await?;
    }

    Ok(WithTempDir {
        inner: wallet,
        temp_dir,
    })
}

/// Funds the `recipients` with gas objects with `amount` or with [`DEFAULT_FUNDING_PER_COIN`]
/// SUI each if no amount is provided.
pub async fn fund_addresses(
    funding_wallet: &mut Wallet,
    recipients: Vec<SuiAddress>,
    amount: Option<u64>,
) -> anyhow::Result<()> {
    let sender = funding_wallet.active_address()?;

    #[allow(deprecated)]
    let gas_coin = funding_wallet
        .gas_for_owner_budget(sender, DEFAULT_GAS_BUDGET, BTreeSet::new())
        .await?
        .1
        .object_ref();

    let mut ptb = ProgrammableTransactionBuilder::new();

    let amount = amount.unwrap_or(DEFAULT_FUNDING_PER_COIN);
    let amounts = vec![amount; recipients.len()];
    ptb.pay_sui(recipients, amounts)?;

    #[allow(deprecated)]
    let reference_gas_price = funding_wallet.get_reference_gas_price().await?;

    let transaction = TransactionData::new_programmable(
        sender,
        vec![gas_coin],
        ptb.finish(),
        DEFAULT_GAS_BUDGET,
        reference_gas_price,
    );
    #[allow(deprecated)]
    funding_wallet
        .execute_transaction_may_fail(funding_wallet.sign_transaction(&transaction).await)
        .await?;

    Ok(())
}

/// Trait to provide an event with the specified `blob_id` for testing.
pub trait EventForTesting: Sized {
    /// Returns an event with the specified `blob_id` and `object_id` for testing.
    fn for_testing_with_object_id(blob_id: BlobId, object_id: ObjectID) -> Self;

    /// Returns an event with the specified `blob_id` and a fixed object ID for testing.
    fn for_testing(blob_id: BlobId) -> Self {
        Self::for_testing_with_object_id(blob_id, FIXED_OBJECT_ID)
    }

    /// Returns an event with the specified `blob_id` and a random object ID for testing.
    fn for_testing_with_random_object_id(blob_id: BlobId) -> Self {
        Self::for_testing_with_object_id(blob_id, ObjectID::random())
    }
}

impl EventForTesting for BlobRegistered {
    fn for_testing_with_object_id(blob_id: BlobId, object_id: ObjectID) -> Self {
        Self {
            epoch: 1,
            blob_id,
            size: 10000,
            encoding_type: DEFAULT_ENCODING,
            end_epoch: 42,
            deletable: false,
            object_id,
            event_id: event_id_for_testing(),
        }
    }
}

impl EventForTesting for BlobCertified {
    fn for_testing_with_object_id(blob_id: BlobId, object_id: ObjectID) -> Self {
        Self {
            epoch: 1,
            blob_id,
            end_epoch: 42,
            deletable: false,
            object_id,
            is_extension: false,
            event_id: event_id_for_testing(),
        }
    }
}

impl EventForTesting for BlobDeleted {
    fn for_testing_with_object_id(blob_id: BlobId, object_id: ObjectID) -> Self {
        Self {
            epoch: 1,
            blob_id,
            end_epoch: 42,
            object_id,
            was_certified: true,
            event_id: event_id_for_testing(),
        }
    }
}

impl EventForTesting for InvalidBlobId {
    /// This should not be used as an `InvalidBlobId` event is not associated with an object ID.
    /// Use [`Self::for_testing`] instead.
    fn for_testing_with_object_id(blob_id: BlobId, _object_id: ObjectID) -> Self {
        Self {
            epoch: 1,
            blob_id,
            event_id: event_id_for_testing(),
        }
    }
}

/// Creates a new StorageNode object representing on chain storage node for testing.
pub fn new_move_storage_node_for_testing() -> StorageNode {
    StorageNode {
        name: "test".to_string(),
        node_id: ObjectID::random(),
        network_address: NetworkAddress("127.0.0.1:8080".to_string()),
        public_key: ProtocolKeyPair::generate().public().clone(),
        next_epoch_public_key: None,
        network_public_key: NetworkKeyPair::generate().public().clone(),
        metadata: ObjectID::random(),
        shard_ids: vec![],
    }
}

impl BlobRegistered {
    /// Creates a certified event with the same parameters as the provided registered event (but a
    /// new event ID) for testing.
    pub fn into_corresponding_certified_event_for_testing(self) -> BlobCertified {
        let Self {
            epoch,
            blob_id,
            end_epoch,
            deletable,
            object_id,
            ..
        } = self;
        BlobCertified {
            epoch,
            blob_id,
            end_epoch,
            deletable,
            object_id,
            is_extension: false,
            event_id: event_id_for_testing(),
        }
    }
}

/// A sorted list of BLS keys for a walrus committee that can be used
/// to certify protocol messages for testing the contracts without
/// running a full walrus system.
#[derive(Debug)]
pub struct TestNodeKeys {
    sorted_keys: Vec<ProtocolKeyPair>,
}

impl TestNodeKeys {
    /// Creates a new set of sorted keys for committee members.
    pub fn new(keys: Vec<ProtocolKeyPair>, committee: &Committee) -> anyhow::Result<Self> {
        let mut key_map: HashMap<_, _> = keys
            .into_iter()
            .map(|key| (key.public().as_bytes().to_owned(), key))
            .collect();
        let sorted_keys = committee
            .members()
            .iter()
            .map(|member| {
                key_map
                    .remove(member.public_key.as_bytes())
                    .ok_or_else(|| anyhow!("no private key provided for committee member"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self { sorted_keys })
    }

    fn certificate_for_signers<T, M>(
        &self,
        message: &T,
        signers: &[u16],
    ) -> anyhow::Result<ProtocolMessageCertificate<T>>
    where
        T: AsRef<ProtocolMessage<M>> + Serialize,
    {
        let signed_messages = signers
            .iter()
            .map(|index| self.sorted_keys[*index as usize].sign_message(message));
        let certificate = ProtocolMessageCertificate::from_signed_messages_and_indices(
            signed_messages,
            signers.into(),
        )?;
        Ok(certificate)
    }

    /// Returns a storage confirmation certificate on the provided `blob_id` and `epoch` from
    /// the test committee, signed by the nodes with indices in `signers`.
    ///
    /// The blob is certified as permanent.
    pub fn blob_certificate_for_signers(
        &self,
        signers: &[u16],
        blob_id: BlobId,
        epoch: Epoch,
    ) -> anyhow::Result<ConfirmationCertificate> {
        let confirmation = Confirmation::new(epoch, blob_id, BlobPersistenceType::Permanent);
        self.certificate_for_signers(&confirmation, signers)
    }

    /// Returns a certificate from the test committee that marks `blob_id` as invalid, signed
    /// by the nodes with indices in `signers`.
    pub fn invalid_blob_certificate_for_signers(
        &self,
        signers: &[u16],
        blob_id: BlobId,
        epoch: Epoch,
    ) -> anyhow::Result<InvalidBlobCertificate> {
        let invalid_blob_id_msg = InvalidBlobIdMsg::new(epoch, blob_id);
        self.certificate_for_signers(&invalid_blob_id_msg, signers)
    }

    /// Returns the key pairs.
    pub fn keys(&self) -> &[ProtocolKeyPair] {
        &self.sorted_keys
    }
}
