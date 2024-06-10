// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use futures::{future::Either, stream::FuturesUnordered, StreamExt};
use sui_types::event::EventID;
use tracing::instrument;
use typed_store::TypedStoreError;
use walrus_core::{
    encoding::{EncodingAxis, EncodingConfig, Primary, Secondary},
    metadata::VerifiedBlobMetadataWithId,
    BlobId,
    InconsistencyProof,
    ShardIndex,
    Sliver,
    SliverPairIndex,
};
use walrus_sui::types::BlobCertified;

use super::StorageNodeInner;
use crate::{
    committee::CommitteeService,
    contract_service::SystemContractService,
    storage::Storage,
};

#[derive(Debug)]
pub(super) struct BlobSynchronizer {
    blob_id: BlobId,
    // TODO(jsmith): Consider making this a weak pointer.
    node: Arc<StorageNodeInner>,
    cursor: (usize, EventID),
}

impl BlobSynchronizer {
    pub fn new(
        event: BlobCertified,
        event_sequence_number: usize,
        node: Arc<StorageNodeInner>,
    ) -> Self {
        Self {
            blob_id: event.blob_id,
            node,
            cursor: (event_sequence_number, event.event_id),
        }
    }

    fn storage(&self) -> &Storage {
        &self.node.storage
    }

    fn encoding_config(&self) -> &EncodingConfig {
        &self.node.encoding_config
    }

    fn committee_service(&self) -> &dyn CommitteeService {
        self.node.committee_service.as_ref()
    }

    fn contract_service(&self) -> &dyn SystemContractService {
        self.node.contract_service.as_ref()
    }

    #[tracing::instrument(skip_all, fields(blob_id = %self.blob_id))]
    pub async fn sync(self) -> anyhow::Result<()> {
        let metadata = self.sync_metadata().await?;

        let mut sliver_sync_futures: FuturesUnordered<_> = self
            .storage()
            .shards()
            .iter()
            .flat_map(|&shard| {
                [
                    Either::Left(self.sync_sliver::<Primary>(shard, &metadata)),
                    Either::Right(self.sync_sliver::<Secondary>(shard, &metadata)),
                ]
            })
            .collect();

        while let Some(result) = sliver_sync_futures.next().await {
            match result {
                Ok(Some(inconsistency_proof)) => {
                    tracing::warn!("received an inconsistency proof");
                    // No need to continue the other futures, sync the inconsistency proof
                    // and return
                    self.sync_inconsistency_proof(&inconsistency_proof).await;
                    break;
                }
                Err(err) => panic!("database operations should not fail: {:?}", err),
                _ => (),
            }
        }

        let (sequence_number, ref event_id) = self.cursor;
        self.node.mark_event_completed(sequence_number, event_id)?;

        Ok(())
    }

    async fn sync_metadata(&self) -> Result<VerifiedBlobMetadataWithId, TypedStoreError> {
        if let Some(metadata) = self.storage().get_metadata(&self.blob_id)? {
            tracing::debug!("not syncing metadata: already stored");
            return Ok(metadata);
        }

        tracing::debug!("syncing metadata");

        let metadata = self
            .node
            .committee_service
            .get_and_verify_metadata(&self.blob_id, &self.node.encoding_config)
            .await;

        self.storage().put_verified_metadata(&metadata)?;

        tracing::debug!("metadata successfully synced");
        Ok(metadata)
    }

    #[instrument(skip_all, fields(axis = ?A::default()))]
    async fn sync_sliver<A: EncodingAxis>(
        &self,
        shard: ShardIndex,
        metadata: &VerifiedBlobMetadataWithId,
    ) -> Result<Option<InconsistencyProof>, TypedStoreError> {
        let shard_storage = self
            .storage()
            .shard_storage(shard)
            .expect("shard is managed by this node");

        // TODO(jsmith): Persist sync across reboots (#395)
        // Handling certified messages does not work for handling reboots etc,
        // because the event is already recorded. We need a way to scan and see of all the blobs we
        // know about, which are stored and which are not fully stored.
        if shard_storage.is_sliver_stored::<A>(&self.blob_id)? {
            tracing::debug!("not syncing sliver: already stored");
            return Ok(None);
        }

        let sliver_id = shard.to_pair_index(self.encoding_config().n_shards(), &self.blob_id);
        let sliver_or_proof = recover_sliver::<A>(
            self.committee_service(),
            metadata,
            sliver_id,
            self.encoding_config(),
        )
        .await;

        match sliver_or_proof {
            Ok(sliver) => {
                shard_storage.put_sliver(&self.blob_id, &sliver)?;
                tracing::debug!("sliver successfully synced");
                Ok(None)
            }
            Err(proof) => Ok(Some(proof)),
        }
    }

    async fn sync_inconsistency_proof(&self, inconsistency_proof: &InconsistencyProof) {
        let invalid_blob_certificate = self
            .committee_service()
            .get_invalid_blob_certificate(
                &self.blob_id,
                inconsistency_proof,
                self.encoding_config().n_shards(),
            )
            .await;
        self.contract_service()
            .invalidate_blob_id(&invalid_blob_certificate)
            .await
    }
}

async fn recover_sliver<A: EncodingAxis>(
    committee_service: &dyn CommitteeService,
    metadata: &VerifiedBlobMetadataWithId,
    sliver_id: SliverPairIndex,
    encoding_config: &EncodingConfig,
) -> Result<Sliver, InconsistencyProof> {
    if A::IS_PRIMARY {
        committee_service
            .recover_primary_sliver(metadata, sliver_id, encoding_config)
            .await
            .map(Sliver::Primary)
            .map_err(InconsistencyProof::Primary)
    } else {
        committee_service
            .recover_secondary_sliver(metadata, sliver_id, encoding_config)
            .await
            .map(Sliver::Secondary)
            .map_err(InconsistencyProof::Secondary)
    }
}
