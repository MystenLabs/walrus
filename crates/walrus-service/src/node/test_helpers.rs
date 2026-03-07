// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Shared test helpers for storage-node tests.
//!
//! This module is a child of `node`, so it can access all of `node`'s internals via `super::`.
//! It is gated on `#[cfg(any(test, feature = "test-utils"))]` so that integration tests
//! (in `tests/`) can use it when `test-utils` is enabled.

use std::{
    sync::{Arc, OnceLock},
    time::Duration,
};

use anyhow::bail;
use sui_types::base_types::ObjectID;
use tokio::sync::{Mutex, broadcast::Sender};
use walrus_core::{
    BlobId,
    DEFAULT_ENCODING,
    Epoch,
    ShardIndex,
    Sliver,
    SliverType,
    encoding::{
        EncodingAxis,
        EncodingConfig,
        EncodingFactory as _,
        Primary,
        Secondary,
        SliverData,
        SliverPair,
    },
    metadata::{UnverifiedBlobMetadataWithId, VerifiedBlobMetadataWithId},
};
use walrus_sdk::sui::types::{ContractEvent, EpochChangeDone, EpochChangeEvent, EpochChangeStart};
use walrus_storage_node_client::StorageNodeClient;
pub use walrus_storage_node_client::api::{BlobStatus, StoredOnNodeStatus};
use walrus_sui::{
    test_utils::{EventForTesting, event_id_for_testing},
    types::{BlobCertified, BlobRegistered},
};
use walrus_test_utils::Result as TestResult;

use super::*;
// Re-export types that integration tests need.
// Only types that are `pub` (not `pub(crate)`) can be re-exported here.
pub use super::{
    ServiceState,
    StorageNodeInner,
    shard_sync::ShardSyncHandler,
    start_epoch_change_finisher::StartEpochChangeFinisher,
    storage::blob_info::{BlobInfo, BlobInfoApi, CertifiedBlobInfoApi},
    storage::{NodeStatus, ShardStatus, ShardStorage, Storage},
};
pub use crate::test_utils::StorageNodeHandleTrait;
use crate::test_utils::{StorageNodeHandle, TestCluster};

// ── Accessor impl blocks for types that need test access ────────────────────

impl StorageNode {
    pub fn inner_for_test(&self) -> &Arc<StorageNodeInner> {
        &self.inner
    }

    pub fn shard_sync_handler_for_test(&self) -> &ShardSyncHandler {
        &self.shard_sync_handler
    }

    pub fn start_epoch_change_finisher_for_test(&self) -> &StartEpochChangeFinisher {
        &self.start_epoch_change_finisher
    }

    pub fn blob_sync_in_progress_for_test(&self) -> std::collections::HashSet<BlobId> {
        self.blob_sync_handler.blob_sync_in_progress()
    }
}

impl StorageNodeInner {
    pub fn storage_for_test(&self) -> &Storage {
        &self.storage
    }

    pub fn encoding_config_for_test(&self) -> &Arc<EncodingConfig> {
        &self.encoding_config
    }

    pub async fn pending_metadata_entry_count_for_test(&self) -> usize {
        self.pending_metadata_cache.entry_count().await
    }

    pub async fn pending_sliver_count_for_test(&self) -> usize {
        self.pending_sliver_cache.sliver_count().await
    }

    pub fn current_committee_epoch_for_test(&self) -> Epoch {
        self.current_committee_epoch()
    }

    pub fn is_blob_certified_for_test(&self, blob_id: &BlobId) -> Result<bool, anyhow::Error> {
        self.is_blob_certified(blob_id)
    }

    pub async fn is_stored_at_all_shards_at_latest_epoch_for_test(
        &self,
        blob_id: &BlobId,
    ) -> anyhow::Result<bool> {
        self.is_stored_at_all_shards_at_latest_epoch(blob_id).await
    }

    pub async fn check_does_not_store_for_blob_for_test(
        &self,
        blob_id: &BlobId,
    ) -> anyhow::Result<()> {
        self.check_does_not_store_metadata_or_slivers_for_blob(blob_id)
            .await
    }

    pub async fn flush_pending_metadata_for_test(&self, blob_id: &BlobId) -> anyhow::Result<()> {
        self.flush_pending_metadata(blob_id)
            .await
            .map_err(Into::into)
    }

    pub async fn flush_pending_slivers_for_test(
        &self,
        blob_id: &BlobId,
        metadata: Arc<VerifiedBlobMetadataWithId>,
    ) -> anyhow::Result<()> {
        self.flush_pending_slivers(blob_id, metadata)
            .await
            .map_err(Into::into)
    }

    pub fn set_node_status_for_test(
        &self,
        status: NodeStatus,
    ) -> Result<(), typed_store::TypedStoreError> {
        self.storage.set_node_status(status)
    }
}

// ── Constants ───────────────────────────────────────────────────────────────

/// Allow a bit more slack now that live-upload deferrals can delay recovery.
pub const TIMEOUT: Duration = Duration::from_secs(5);

pub const OTHER_BLOB_ID: BlobId = BlobId([247; 32]);

pub const BLOB: &[u8] = &[
    0, 1, 255, 0, 2, 254, 0, 3, 253, 0, 4, 252, 0, 5, 251, 0, 6, 250, 0, 7, 249, 0, 8, 248,
];

/// Mirrors `storage::tests::BLOB_ID`.
pub const BLOB_ID: BlobId = BlobId([7; 32]);

/// Mirrors `storage::tests::SHARD_INDEX`.
pub const SHARD_INDEX: ShardIndex = ShardIndex(3);

// ── Helper structs ──────────────────────────────────────────────────────────

#[derive(Debug)]
pub struct EncodedBlob {
    pub config: EncodingConfig,
    pub pairs: Vec<SliverPair>,
    pub metadata: VerifiedBlobMetadataWithId,
    #[allow(unused)]
    pub object_id: Option<ObjectID>,
}

impl EncodedBlob {
    pub fn new(blob: &[u8], config: EncodingConfig) -> Self {
        let (pairs, metadata) = config
            .get_for_type(DEFAULT_ENCODING)
            .encode_with_metadata(blob.to_vec())
            .expect("must be able to get encoder");

        Self {
            pairs,
            metadata,
            config,
            object_id: None,
        }
    }

    pub fn new_with_object_id(blob: &[u8], config: EncodingConfig, object_id: ObjectID) -> Self {
        Self {
            object_id: Some(object_id),
            ..Self::new(blob, config)
        }
    }

    pub fn blob_id(&self) -> &BlobId {
        self.metadata.blob_id()
    }

    pub fn assigned_sliver_pair(&self, shard: ShardIndex) -> &SliverPair {
        let pair_index = shard.to_pair_index(self.config.n_shards(), self.blob_id());
        self.pairs
            .iter()
            .find(|pair| pair.index() == pair_index)
            .expect("shard must be assigned at least 1 sliver")
    }
}

pub struct ShardStorageSet {
    pub shard_storage: Vec<Arc<ShardStorage>>,
}

/// A struct that contains the event senders for each node in the cluster.
#[allow(unused)]
pub struct ClusterEventSenders {
    /// The event sender for node 0.
    pub node_0_events: Sender<ContractEvent>,
    /// The event sender for all other nodes.
    pub all_other_node_events: Sender<ContractEvent>,
}

// ── Helper functions ────────────────────────────────────────────────────────

/// Prevent tests running simultaneously to avoid interferences or race conditions.
pub fn global_test_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(Mutex::default)
}

pub async fn store_at_shards<F>(
    blob: &EncodedBlob,
    cluster: &TestCluster,
    mut store_at_shard: F,
) -> TestResult
where
    F: FnMut(&ShardIndex, SliverType) -> bool,
{
    let mut nodes_and_shards = Vec::new();
    for node in cluster.nodes.iter() {
        let existing_shards = node.storage_node().existing_shards().await;
        nodes_and_shards.extend(std::iter::repeat(node).zip(existing_shards));
    }

    let mut metadata_stored = vec![];

    for (node, shard) in nodes_and_shards {
        if !metadata_stored.contains(&node.public_key())
            && (store_at_shard(&shard, SliverType::Primary)
                || store_at_shard(&shard, SliverType::Secondary))
        {
            crate::test_utils::retry_until_success_or_timeout(TIMEOUT, || {
                node.client().store_metadata(
                    &blob.metadata,
                    walrus_storage_node_client::UploadIntent::Immediate,
                )
            })
            .await?;
            metadata_stored.push(node.public_key());
        }

        let sliver_pair = blob.assigned_sliver_pair(shard);

        if store_at_shard(&shard, SliverType::Primary) {
            node.client()
                .store_sliver(
                    blob.blob_id(),
                    sliver_pair.index(),
                    &sliver_pair.primary,
                    walrus_storage_node_client::UploadIntent::Immediate,
                )
                .await?;
        }

        if store_at_shard(&shard, SliverType::Secondary) {
            node.client()
                .store_sliver(
                    blob.blob_id(),
                    sliver_pair.index(),
                    &sliver_pair.secondary,
                    walrus_storage_node_client::UploadIntent::Immediate,
                )
                .await?;
        }
    }

    Ok(())
}

pub async fn cluster_at_epoch1_without_blobs(
    assignment: &[&[u16]],
    shard_sync_config: Option<config::ShardSyncConfig>,
) -> TestResult<(TestCluster, Sender<ContractEvent>)> {
    let events = Sender::new(48);

    let cluster: TestCluster = {
        // Lock to avoid race conditions.
        let _lock = global_test_lock().lock().await;
        let mut builder = TestCluster::<StorageNodeHandle>::builder()
            .with_shard_assignment(assignment)
            .with_system_event_providers(events.clone());
        if let Some(shard_sync_config) = shard_sync_config {
            builder = builder.with_shard_sync_config(shard_sync_config);
        }
        builder.build().await?
    };

    // Explicitly emit epoch 1 transition so nodes process EpochChangeStart/Done,
    // regardless of the initial epoch of the committee service.
    events.send(ContractEvent::EpochChangeEvent(
        EpochChangeEvent::EpochChangeStart(EpochChangeStart {
            epoch: 1,
            event_id: event_id_for_testing(),
        }),
    ))?;
    events.send(ContractEvent::EpochChangeEvent(
        EpochChangeEvent::EpochChangeDone(EpochChangeDone {
            epoch: 1,
            event_id: event_id_for_testing(),
        }),
    ))?;

    Ok((cluster, events))
}

pub async fn cluster_at_epoch1_without_blobs_waiting_for_active_nodes(
    assignment: &[&[u16]],
    shard_sync_config: Option<config::ShardSyncConfig>,
) -> TestResult<(TestCluster, Sender<ContractEvent>)> {
    let (cluster, events) = cluster_at_epoch1_without_blobs(assignment, shard_sync_config).await?;

    // Wait until nodes reach epoch 1 and become Active.
    cluster.wait_for_nodes_to_reach_epoch(1).await;
    crate::test_utils::retry_until_success_or_timeout(
        std::time::Duration::from_secs(10),
        || async {
            for (i, node) in cluster.nodes.iter().enumerate() {
                let status = node
                    .storage_node()
                    .inner
                    .storage
                    .node_status()
                    .map_err(|e: typed_store::TypedStoreError| anyhow::anyhow!(e))?;
                if status != NodeStatus::Active {
                    anyhow::bail!("node {i} not Active yet");
                }
            }
            Ok(())
        },
    )
    .await
    .context("failed to wait for nodes to set state to 'Active' in epoch 1")?;

    Ok((cluster, events))
}

pub async fn cluster_with_partially_stored_blob<F>(
    assignment: &[&[u16]],
    blob: &[u8],
    store_at_shard: F,
) -> TestResult<(TestCluster, Sender<ContractEvent>, EncodedBlob)>
where
    F: FnMut(&ShardIndex, SliverType) -> bool,
{
    let (cluster, events) = cluster_at_epoch1_without_blobs(assignment, None).await?;

    let config = cluster.encoding_config();
    let mut blob_details = EncodedBlob::new(blob, config);

    let registered_event = BlobRegistered::for_testing(*blob_details.blob_id());
    blob_details.object_id = Some(registered_event.object_id);
    events.send(registered_event.into())?;
    store_at_shards(&blob_details, &cluster, store_at_shard).await?;

    Ok((cluster, events, blob_details))
}

/// Creates a test cluster with custom initial epoch and blobs that are already certified.
pub async fn cluster_with_initial_epoch_and_certified_blobs(
    assignment: &[&[u16]],
    blobs: &[&[u8]],
    initial_epoch: Epoch,
    shard_sync_config: Option<config::ShardSyncConfig>,
) -> TestResult<(TestCluster, Sender<ContractEvent>, Vec<EncodedBlob>)> {
    let (cluster, events) = cluster_at_epoch1_without_blobs(assignment, shard_sync_config).await?;

    let config = cluster.encoding_config();
    let mut details = Vec::new();

    // Add the blobs at epoch 1, the epoch at which the cluster starts.
    for blob in blobs {
        let blob_details = EncodedBlob::new(blob, config.clone());
        let object_id = ObjectID::random();
        // Note: register and certify the blob are always using epoch 0.
        events.send(
            BlobRegistered::for_testing_with_object_id(*blob_details.blob_id(), object_id).into(),
        )?;
        store_at_shards(&blob_details, &cluster, |_, _| true).await?;
        events.send(
            BlobCertified::for_testing_with_object_id(*blob_details.blob_id(), object_id).into(),
        )?;
        details.push(blob_details);
    }

    advance_cluster_to_epoch(&cluster, &[&events], initial_epoch).await?;

    Ok((cluster, events, details))
}

pub async fn advance_cluster_to_epoch(
    cluster: &TestCluster,
    events: &[&Sender<ContractEvent>],
    epoch: Epoch,
) -> TestResult {
    let lookup_service_handle = cluster.lookup_service_handle.clone().unwrap();
    for epoch in lookup_service_handle.epoch() + 1..epoch + 1 {
        let new_epoch = lookup_service_handle.advance_epoch();
        assert_eq!(new_epoch, epoch);
        for event_queue in events {
            event_queue.send(ContractEvent::EpochChangeEvent(
                EpochChangeEvent::EpochChangeStart(EpochChangeStart {
                    epoch,
                    event_id: event_id_for_testing(),
                }),
            ))?;
            event_queue.send(ContractEvent::EpochChangeEvent(
                EpochChangeEvent::EpochChangeDone(EpochChangeDone {
                    epoch,
                    event_id: event_id_for_testing(),
                }),
            ))?;
        }
        cluster.wait_for_nodes_to_reach_epoch(epoch).await;
    }

    Ok(())
}

/// Creates a test cluster with custom initial epoch and blobs that are partially stored
/// in shard 0.
///
/// The function is created for testing shard syncing/recovery. So for blobs that are
/// not stored in shard 0, it also won't receive a certified event.
///
/// The function also takes custom function to determine the end epoch of a blob, and whether
/// the blob should be deletable.
pub async fn cluster_with_partially_stored_blobs_in_shard_0<F, G, H>(
    assignment: &[&[u16]],
    blobs: &[&[u8]],
    initial_epoch: Epoch,
    mut blob_index_store_at_shard_0: F,
    mut blob_index_to_end_epoch: G,
    mut blob_index_to_deletable: H,
) -> TestResult<(TestCluster, Vec<EncodedBlob>, ClusterEventSenders)>
where
    F: FnMut(usize) -> bool,
    G: FnMut(usize) -> Epoch,
    H: FnMut(usize) -> bool,
{
    // Node 0 must contain shard 0.
    assert!(assignment[0].contains(&0));

    // Create event providers for each node.
    let node_0_events = Sender::new(48);
    let all_other_node_events = Sender::new(48);
    let event_providers = vec![node_0_events.clone(); 1]
        .into_iter()
        .chain(vec![all_other_node_events.clone(); assignment.len() - 1].into_iter())
        .collect::<Vec<_>>();

    let cluster = {
        // Lock to avoid race conditions.
        let _lock = global_test_lock().lock().await;
        TestCluster::<StorageNodeHandle>::builder()
            .with_shard_assignment(assignment)
            .with_individual_system_event_providers(&event_providers)
            .build()
            .await?
    };

    let config = cluster.encoding_config();
    let mut details = Vec::new();
    for (i, blob) in blobs.iter().enumerate() {
        let object_id = ObjectID::random();
        let blob_details = EncodedBlob::new_with_object_id(blob, config.clone(), object_id);
        let blob_end_epoch = blob_index_to_end_epoch(i);
        let deletable = blob_index_to_deletable(i);
        let blob_registration_event = BlobRegistered {
            deletable,
            end_epoch: blob_end_epoch,
            ..BlobRegistered::for_testing_with_object_id(*blob_details.blob_id(), object_id)
        };
        let blob_certified_event = blob_registration_event
            .clone()
            .into_corresponding_certified_event_for_testing();
        node_0_events.send(blob_registration_event.clone().into())?;
        all_other_node_events.send(blob_registration_event.into())?;

        if blob_index_store_at_shard_0(i) {
            store_at_shards(&blob_details, &cluster, |_, _| true).await?;
            node_0_events.send(blob_certified_event.clone().into())?;
        } else {
            // Don't certify the blob if it's not stored in shard 0.
            store_at_shards(&blob_details, &cluster, |shard_index, _| {
                shard_index != &ShardIndex(0)
            })
            .await?;
        }

        all_other_node_events.send(blob_certified_event.into())?;
        details.push(blob_details);
    }

    advance_cluster_to_epoch(
        &cluster,
        &[&node_0_events, &all_other_node_events],
        initial_epoch,
    )
    .await?;

    Ok((
        cluster,
        details,
        ClusterEventSenders {
            node_0_events,
            all_other_node_events,
        },
    ))
}

/// The common setup for shard sync tests.
pub async fn setup_cluster_for_shard_sync_tests(
    assignment: Option<&[&[u16]]>,
    shard_sync_config: Option<config::ShardSyncConfig>,
) -> TestResult<(TestCluster, Vec<EncodedBlob>, Storage, Arc<ShardStorageSet>)> {
    let assignment = assignment.unwrap_or(&[&[0], &[1, 2, 3]]);
    let blobs: Vec<[u8; 32]> = (1..24).map(|i| [i; 32]).collect();
    let blobs: Vec<_> = blobs.iter().map(|b| &b[..]).collect();
    let (cluster, _, blob_details) =
        cluster_with_initial_epoch_and_certified_blobs(assignment, &blobs, 2, shard_sync_config)
            .await?;

    // Makes storage inner mutable so that we can manually add another shard to node 1.
    let node_inner = unsafe {
        &mut *(Arc::as_ptr(&cluster.nodes[1].storage_node.inner) as *mut StorageNodeInner)
    };
    let shard_indices: Vec<_> = assignment[0].iter().map(|i| ShardIndex(*i)).collect();
    node_inner
        .storage
        .create_storage_for_shards(&shard_indices)
        .await?;
    let mut shard_storage = vec![];
    for shard_index in shard_indices {
        shard_storage.push(
            node_inner
                .storage
                .shard_storage(shard_index)
                .await
                .expect("shard storage should exist"),
        );
    }

    let shard_storage_set = ShardStorageSet { shard_storage };
    let shard_storage_set = Arc::new(shard_storage_set);

    for shard_storage in shard_storage_set.shard_storage.iter() {
        shard_storage
            .update_status_in_test(ShardStatus::None)
            .await?;
    }

    Ok((
        cluster,
        blob_details,
        node_inner.storage.clone(),
        shard_storage_set.clone(),
    ))
}

/// Checks that all primary and secondary slivers match the original encoding of the blobs.
/// Checks that blobs in the skip list are not synced.
pub fn check_all_blobs_are_synced(
    blob_details: &[EncodedBlob],
    storage_dst: &Storage,
    shard_storage_dst: &ShardStorage,
    skip_blob_indices: &[usize],
) -> anyhow::Result<()> {
    blob_details
        .iter()
        .enumerate()
        .try_for_each(|(i, details)| {
            let blob_id = *details.blob_id();

            // If the blob is in the skip list, it should not be present in the destination
            // shard storage.
            if skip_blob_indices.contains(&i) {
                assert!(
                    shard_storage_dst
                        .get_sliver(&blob_id, SliverType::Primary)
                        .unwrap()
                        .is_none()
                );
                assert!(
                    shard_storage_dst
                        .get_sliver(&blob_id, SliverType::Secondary)
                        .unwrap()
                        .is_none()
                );
                return Ok::<(), anyhow::Error>(());
            }

            let Sliver::Primary(dst_primary) = shard_storage_dst
                .get_sliver(&blob_id, SliverType::Primary)
                .unwrap()
                .unwrap()
            else {
                panic!("Must get primary sliver");
            };
            let Sliver::Secondary(dst_secondary) = shard_storage_dst
                .get_sliver(&blob_id, SliverType::Secondary)
                .unwrap()
                .unwrap()
            else {
                panic!("Must get secondary sliver");
            };

            assert_eq!(
                details.assigned_sliver_pair(ShardIndex(0)),
                &SliverPair {
                    primary: dst_primary,
                    secondary: dst_secondary,
                }
            );

            // Check that metadata is synced.
            assert_eq!(
                details.metadata,
                storage_dst.get_metadata(&blob_id).unwrap().unwrap(),
            );

            Ok(())
        })?;
    Ok(())
}

/// Waits for the shard to be synced.
pub async fn wait_for_shard_in_active_state(shard_storage: &ShardStorage) -> TestResult {
    let timeout = if cfg!(tarpaulin) {
        Duration::from_secs(30)
    } else {
        Duration::from_secs(15)
    };
    tokio::time::timeout(timeout, async {
        loop {
            let status = shard_storage.status().await.unwrap();
            if status == ShardStatus::Active {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await?;

    Ok(())
}

pub async fn wait_for_shards_in_active_state(shard_storage_set: &ShardStorageSet) -> TestResult {
    // Waits for the shard to be synced.
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let mut all_active = true;
            for shard_storage in &shard_storage_set.shard_storage {
                let status = shard_storage
                    .status()
                    .await
                    .expect("Shard status should be present");
                if status != ShardStatus::Active {
                    all_active = false;
                    break;
                }
            }
            if all_active {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await?;

    Ok(())
}

/// Sets up a test cluster for shard recovery tests.
pub async fn setup_shard_recovery_test_cluster_with_blob_count<F, G, H>(
    blob_count: u8,
    blob_index_store_at_shard_0: F,
    blob_index_to_end_epoch: G,
    blob_index_to_deletable: H,
) -> TestResult<(TestCluster, Vec<EncodedBlob>, ClusterEventSenders)>
where
    F: FnMut(usize) -> bool,
    G: FnMut(usize) -> Epoch,
    H: FnMut(usize) -> bool,
{
    let blobs: Vec<[u8; 32]> = (1..=blob_count).map(|i| [i; 32]).collect();
    let blobs: Vec<_> = blobs.iter().map(|b| &b[..]).collect();
    let (cluster, blob_details, event_senders) = cluster_with_partially_stored_blobs_in_shard_0(
        &[&[0], &[1, 2, 3, 4], &[5, 6, 7, 8, 9]],
        &blobs,
        2,
        blob_index_store_at_shard_0,
        blob_index_to_end_epoch,
        blob_index_to_deletable,
    )
    .await?;

    Ok((cluster, blob_details, event_senders))
}

pub async fn setup_shard_recovery_test_cluster<F, G, H>(
    blob_index_store_at_shard_0: F,
    blob_index_to_end_epoch: G,
    blob_index_to_deletable: H,
) -> TestResult<(TestCluster, Vec<EncodedBlob>, ClusterEventSenders)>
where
    F: FnMut(usize) -> bool,
    G: FnMut(usize) -> Epoch,
    H: FnMut(usize) -> bool,
{
    setup_shard_recovery_test_cluster_with_blob_count(
        23,
        blob_index_store_at_shard_0,
        blob_index_to_end_epoch,
        blob_index_to_deletable,
    )
    .await
}

pub async fn expect_sliver_pair_stored_before_timeout(
    blob: &EncodedBlob,
    node_client: &StorageNodeClient,
    shard: ShardIndex,
    timeout: Duration,
) -> SliverPair {
    let (primary, secondary) = tokio::join!(
        expect_sliver_stored_before_timeout::<Primary>(blob, node_client, shard, timeout,),
        expect_sliver_stored_before_timeout::<Secondary>(blob, node_client, shard, timeout,)
    );

    SliverPair { primary, secondary }
}

pub async fn expect_sliver_stored_before_timeout<A: EncodingAxis>(
    blob: &EncodedBlob,
    node_client: &StorageNodeClient,
    shard: ShardIndex,
    timeout: Duration,
) -> SliverData<A> {
    crate::test_utils::retry_until_success_or_timeout(timeout, || {
        let pair_to_sync = blob.assigned_sliver_pair(shard);
        node_client.get_sliver::<A>(blob.blob_id(), pair_to_sync.index())
    })
    .await
    .expect("sliver should be available at some point after being certified")
}

pub async fn set_up_node_with_metadata(
    metadata: UnverifiedBlobMetadataWithId,
) -> anyhow::Result<StorageNodeHandle> {
    let blob_id = metadata.blob_id().to_owned();

    let shards = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9].map(ShardIndex::new);

    // create a storage node with a registered event for the blob id
    let node = StorageNodeHandle::builder()
        .with_system_event_provider(vec![BlobRegistered::for_testing(blob_id).into()])
        .with_shard_assignment(&shards)
        .with_node_started(true)
        .build()
        .await?;

    // make sure that the event is received by the node
    tokio::time::sleep(Duration::from_millis(50)).await;

    // store the metadata in the storage node
    node.as_ref()
        .store_metadata(metadata, UploadIntent::Immediate)
        .await?;

    Ok(node)
}

/// Waits until the storage node processes the specified number of events.
pub async fn wait_until_events_processed(
    node: &StorageNodeHandle,
    processed_event_count: u64,
) -> anyhow::Result<u64> {
    crate::test_utils::retry_until_success_or_timeout(Duration::from_secs(10), || async {
        let count = node
            .storage_node
            .inner
            .storage
            .get_sequentially_processed_event_count()?;
        if count >= processed_event_count {
            Ok(count)
        } else {
            bail!("not enough events processed")
        }
    })
    .await
}

pub async fn wait_until_events_processed_exact(
    node: &StorageNodeHandle,
    processed_event_count: u64,
) -> anyhow::Result<u64> {
    let event_count = wait_until_events_processed(node, processed_event_count).await?;
    if event_count == processed_event_count {
        Ok(event_count)
    } else {
        bail!(
            "too many events processed: {event_count} (actual) > {processed_event_count} \
            (expected)"
        )
    }
}
