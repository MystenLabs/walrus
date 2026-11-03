// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Strata implementation of primary and secondary sliver storage.

use std::{path::Path, sync::Arc, time::Duration};

use strata::{BlobKey, ShardState, StrataLsn, StrataStore, StrataStoreConfig, StrataStoreMetrics};
use typed_store::TypedStoreError;
use walrus_core::{BlobId, ShardIndex, Sliver, SliverType};
use walrus_utils::metrics::Registry;

use super::{PrimarySliverData, SecondarySliverData};
use crate::utils;

// Time in seconds to wait for data to be fsynced when writing slivers into Strata.
const SYNC_WAIT_TIMEOUT: Duration = Duration::from_secs(30);

/// Strata store for slivers. Walrus control tables remain in RocksDB.
#[derive(Debug, Clone)]
pub(super) struct StrataSliverStore {
    store: Arc<StrataStore>,
}

/// One Walrus shard within the Strata store, holding both primary and secondary slivers.
#[derive(Debug, Clone)]
pub(super) struct StrataShardSliverStore {
    node: StrataSliverStore,
    shard: ShardIndex,
}

impl StrataSliverStore {
    pub(super) fn open(path: &Path, metrics_registry: &Registry) -> anyhow::Result<Self> {
        let config = StrataStoreConfig::new(path, "slivers");
        let metrics = StrataStoreMetrics::new(
            metrics_registry.prometheus_registry(),
            format!("slivers:{}", path.display()),
        )?;
        Ok(Self {
            store: Arc::new(StrataStore::open(config, metrics)?),
        })
    }

    pub(super) fn open_shard(
        &self,
        shard: ShardIndex,
    ) -> Result<StrataShardSliverStore, TypedStoreError> {
        self.store
            .add_shard(u32::from(shard.0))
            .map_err(store_error)?;
        Ok(StrataShardSliverStore {
            node: self.clone(),
            shard,
        })
    }

    pub(super) fn shard_state(
        &self,
        shard: ShardIndex,
    ) -> Result<Option<ShardState>, TypedStoreError> {
        self.store
            .shard_info(u32::from(shard.0))
            .map(|info| info.map(|info| info.state))
            .map_err(store_error)
    }

    pub(super) fn contains_sliver_pairs_in_all(
        &self,
        blob_id: BlobId,
        shards: &[ShardIndex],
    ) -> Result<bool, TypedStoreError> {
        let shards = shards
            .iter()
            .map(|shard| u32::from(shard.0))
            .collect::<Vec<_>>();
        for sliver_type in [SliverType::Primary, SliverType::Secondary] {
            let key = sliver_key(&blob_id, sliver_type);
            if !self
                .store
                .contains_in_shards(&key, &shards)
                .map_err(store_error)?
            {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Waits until the Strata checkpoint makes `lsn` crash safe, not merely visible to reads.
    pub(super) async fn wait_durable_lsn(&self, lsn: StrataLsn) -> Result<(), TypedStoreError> {
        if lsn == 0 {
            return Ok(());
        }
        let mut receiver = self.store.subscribe_durability_progress();
        tokio::time::timeout(SYNC_WAIT_TIMEOUT, async move {
            loop {
                let progress = receiver.borrow_and_update().clone();
                if progress.published_lsn >= lsn {
                    return Ok(());
                }
                if let Some(reason) = progress.halt_reason {
                    return Err(TypedStoreError::TaskError(format!(
                        "Strata halted before sliver LSN {lsn} became durable: {reason}"
                    )));
                }
                receiver.changed().await.map_err(|_| {
                    TypedStoreError::TaskError("Strata durability notifier stopped".to_owned())
                })?;
            }
        })
        .await
        .map_err(|_| {
            TypedStoreError::TaskError(format!(
                "timed out waiting for Strata sliver LSN {lsn} to become durable"
            ))
        })?
    }
}

impl StrataShardSliverStore {
    pub(super) fn drop_shard(&self) -> Result<(), TypedStoreError> {
        self.node
            .store
            .drop_shard(u32::from(self.shard.0))
            .map_err(store_error)?;
        // Strata drop returns when the generation fence is visible, not yet crash safe. It
        // does not return an LSN to wait on, so sync before removing RocksDB control tables.
        self.node.store.sync().map_err(store_error)
    }

    pub(super) async fn put(&self, blob_id: BlobId, sliver: Sliver) -> Result<(), TypedStoreError> {
        let this = self.clone();
        let lsn = utils::unwrap_or_resume_unwind(
            tokio::task::spawn_blocking(move || this.put_visible(blob_id, &sliver)).await,
        )?;
        self.node.wait_durable_lsn(lsn).await
    }

    fn put_visible(&self, blob_id: BlobId, sliver: &Sliver) -> Result<StrataLsn, TypedStoreError> {
        let key = sliver_key(&blob_id, sliver.r#type());
        let payload = serialize_sliver(sliver)?;
        self.node
            .store
            .put(u32::from(self.shard.0), &key, &payload)
            .map_err(store_error)
    }

    pub(super) async fn put_many(
        &self,
        slivers: Vec<(BlobId, Sliver)>,
    ) -> Result<(), TypedStoreError> {
        if slivers.is_empty() {
            return Ok(());
        }
        let this = self.clone();
        let lsn = utils::unwrap_or_resume_unwind(
            tokio::task::spawn_blocking(move || {
                let mut batch = this.node.store.batch();
                for (blob_id, sliver) in slivers {
                    let key = sliver_key(&blob_id, sliver.r#type());
                    let payload = serialize_sliver(&sliver)?;
                    batch.put(u32::from(this.shard.0), key, Arc::<[u8]>::from(payload));
                }
                let result = batch.write().map_err(store_error)?;
                Ok::<_, TypedStoreError>(result.op_lsns().last().copied().unwrap_or(0))
            })
            .await,
        )?;
        self.node.wait_durable_lsn(lsn).await
    }

    pub(super) fn get(
        &self,
        blob_id: &BlobId,
        sliver_type: SliverType,
    ) -> Result<Option<Sliver>, TypedStoreError> {
        let key = sliver_key(blob_id, sliver_type);
        self.node
            .store
            .get_from_shard(u32::from(self.shard.0), &key)
            .map_err(store_error)?
            .map(|payload| deserialize_sliver(sliver_type, &payload))
            .transpose()
    }

    pub(super) fn contains(
        &self,
        blob_id: &BlobId,
        sliver_type: SliverType,
    ) -> Result<bool, TypedStoreError> {
        let key = sliver_key(blob_id, sliver_type);
        self.node
            .store
            .contains_in_shards(&key, &[u32::from(self.shard.0)])
            .map_err(store_error)
    }

    pub(super) async fn delete_pair(&self, blob_id: BlobId) -> Result<(), TypedStoreError> {
        let this = self.clone();
        let lsn = utils::unwrap_or_resume_unwind(
            tokio::task::spawn_blocking(move || {
                let mut batch = this.node.store.batch();
                for sliver_type in [SliverType::Primary, SliverType::Secondary] {
                    batch.tombstone(u32::from(this.shard.0), sliver_key(&blob_id, sliver_type));
                }
                let result = batch.write().map_err(store_error)?;
                Ok::<_, TypedStoreError>(result.op_lsns().last().copied().unwrap_or(0))
            })
            .await,
        )?;
        self.node.wait_durable_lsn(lsn).await
    }
}

fn sliver_key(blob_id: &BlobId, sliver_type: SliverType) -> BlobKey {
    let mut bytes = Vec::with_capacity(1 + BlobId::LENGTH);
    bytes.push(if sliver_type.is_primary() { 0 } else { 1 });
    bytes.extend_from_slice(&blob_id.0);
    BlobKey::new(bytes).expect("the fixed-size sliver key is valid")
}

fn serialize_sliver(sliver: &Sliver) -> Result<Vec<u8>, TypedStoreError> {
    match sliver {
        Sliver::Primary(primary) => bcs::to_bytes(&PrimarySliverData::from(primary.clone())),
        Sliver::Secondary(secondary) => {
            bcs::to_bytes(&SecondarySliverData::from(secondary.clone()))
        }
    }
    .map_err(|error| TypedStoreError::SerializationError(error.to_string()))
}

fn deserialize_sliver(sliver_type: SliverType, payload: &[u8]) -> Result<Sliver, TypedStoreError> {
    match sliver_type {
        SliverType::Primary => {
            bcs::from_bytes::<PrimarySliverData>(payload).map(|data| Sliver::Primary(data.into()))
        }
        SliverType::Secondary => bcs::from_bytes::<SecondarySliverData>(payload)
            .map(|data| Sliver::Secondary(data.into())),
    }
    .map_err(|error| TypedStoreError::SerializationError(error.to_string()))
}

fn store_error(error: strata::Error) -> TypedStoreError {
    TypedStoreError::TaskError(format!("Strata sliver store: {error}"))
}
