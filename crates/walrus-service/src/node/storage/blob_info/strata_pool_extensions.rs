// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Expand pool events above Strata, before later metadata events or GC can change membership.

use super::*;
use crate::node::storage::strata_queue::{
    HaltOnIncompleteWrite,
    PendingPoolExtension,
    StrataQueue,
    queue_error,
};

impl BlobInfoTable {
    /// Synchronous producers must not bypass the async event barrier. Replays are filtered first.
    pub(super) fn ensure_pool_extensions_finished(&self) -> Result<(), TypedStoreError> {
        if let Some(registrations) = &self.strata_registrations
            && registrations
                .pools
                .safe_iter()?
                .next()
                .transpose()?
                .is_some()
        {
            return Err(TypedStoreError::TaskError(
                "pending Strata pool extension must finish before later metadata events".into(),
            ));
        }
        Ok(())
    }

    /// Producers wait here before processing later events. The supervised worker also drives
    /// these same persisted jobs when there is no event traffic, and foreground puts can help.
    /// No blob/lifecycle guard may be held by the caller.
    pub(crate) async fn finish_strata_pool_extensions(
        &self,
        queue: &StrataQueue,
    ) -> Result<(), TypedStoreError> {
        if self.strata_registrations.is_none() {
            return Ok(());
        }
        while self.process_strata_pool_extension_batch(queue).await? {}
        Ok(())
    }

    /// A bounded, owned pass: cancelling the waiter cannot abandon a started batch or its locks.
    pub(crate) async fn process_strata_pool_extension_batch(
        &self,
        queue: &StrataQueue,
    ) -> Result<bool, TypedStoreError> {
        let table = self.clone();
        let queue = queue.clone();
        crate::utils::unwrap_or_resume_unwind(
            tokio::spawn(async move {
                let Some(registrations) = table.strata_registrations.clone() else {
                    return Ok(false);
                };
                // Cloned workers/producers share this mutex. Hold no blob lock while waiting.
                let _worker = registrations.pool_worker.lock().await;
                let reader = table.clone();
                let selected = crate::utils::unwrap_or_resume_unwind(
                    tokio::task::spawn_blocking(move || {
                        let Some((_, job)) = reader
                            .strata_registrations
                            .as_ref()
                            .expect("pool worker requires the Strata backend")
                            .pools
                            .safe_iter()?
                            .next()
                            .transpose()?
                        else {
                            return Ok::<_, TypedStoreError>(None);
                        };
                        let start = job.after_object.map_or(Unbounded, Bound::Excluded);
                        let rows = reader
                            .per_object_pooled_blob_info
                            .safe_range_iter((start, Unbounded))?
                            .take(STRATA_POOL_EXTENSION_BATCH_SIZE)
                            .collect::<Result<Vec<_>, _>>()?;
                        Ok(Some((job, rows)))
                    })
                    .await,
                )
                .inspect_err(|error| queue.halt(error.to_string()))?;
                let Some((job, rows)) = selected else {
                    return Ok(false);
                };
                let blobs: HashSet<_> = rows
                    .iter()
                    .filter_map(|(_, PerObjectPooledBlobInfo::V1(info))| {
                        (info.storage_pool_id == job.pool_id).then_some(info.blob_id)
                    })
                    .collect();
                let keys: Vec<_> = blobs.iter().map(|blob| blob.0.as_slice()).collect();
                // Membership is stable because later events and GC wait for this job. Only the
                // affected blobs in this batch need locks; unrelated puts continue during the scan.
                let _guard = queue.lock_blobs(&keys).await.map_err(queue_error)?;
                let mut completion = HaltOnIncompleteWrite::new(queue);
                crate::utils::unwrap_or_resume_unwind(
                    tokio::task::spawn_blocking(move || {
                        table.expand_strata_pool_batch(job, rows, blobs)
                    })
                    .await,
                )?;
                completion.complete();
                Ok(true)
            })
            .await,
        )
    }

    fn expand_strata_pool_batch(
        &self,
        mut job: PendingPoolExtension,
        rows: Vec<(ObjectID, PerObjectPooledBlobInfo)>,
        blobs: HashSet<BlobId>,
    ) -> Result<(), TypedStoreError> {
        let registrations = self
            .strata_registrations
            .as_ref()
            .expect("pool worker requires the Strata backend");
        let mut batch = registrations.pools.batch();
        for blob_id in blobs {
            let registration = registrations.get(&blob_id)?.ok_or_else(|| {
                TypedStoreError::TaskError("Strata pool member has no registration identity".into())
            })?;
            // The conservative maximum and command commit together. Preserve longer references
            // and skip duplicate members in later chunks, even if an earlier command was acked.
            if registration.end_epoch < job.end_epoch {
                registrations.extend(&mut batch, blob_id, job.source.clone(), job.end_epoch)?;
            }
        }
        if rows.len() < STRATA_POOL_EXTENSION_BATCH_SIZE {
            batch.delete_batch(&registrations.pools, [job.source.event_index])?;
        } else {
            job.after_object = rows.last().map(|(object, _)| *object);
            batch.insert_batch(&registrations.pools, [(job.source.event_index, job)])?;
        }
        // Cursor/completion and generated commands share one WAL batch. The Strata worker's
        // snapshot-then-sync includes these commands and their preceding pool metadata before
        // applying anything to Strata. Later durable events also cover this earlier completion.
        batch.write()
    }
}
