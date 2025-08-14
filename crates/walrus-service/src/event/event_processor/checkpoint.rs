// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Checkpoint module for processing checkpoint data.
use std::{
    fmt::{self},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use anyhow::{Result, anyhow};
use futures_util::future::try_join_all;
use move_core_types::annotated_value::{MoveDatatypeLayout, MoveTypeLayout};
use sui_package_resolver::Resolver;
use sui_sdk::rpc_types::SuiEvent;
use sui_types::{
    SYSTEM_PACKAGE_ADDRESSES, base_types::ObjectID, committee::Committee,
    effects::TransactionEffectsAPI, full_checkpoint_content::CheckpointData,
    message_envelope::Message, messages_checkpoint::VerifiedCheckpoint,
};
use typed_store::Map;
use walrus_sui::types::ContractEvent;

use crate::event::{
    event_processor::{
        db::EventProcessorStores,
        package_store::{LocalDBPackageStore, PackageCache},
    },
    events::{CheckpointEventPosition, PositionedStreamEvent},
};

/// A struct that processes checkpoint data.
#[derive(Clone)]
pub struct CheckpointProcessor {
    stores: EventProcessorStores,
    package_store: LocalDBPackageStore,
    package_resolver: Arc<Resolver<PackageCache>>,
    original_system_pkg_id: ObjectID,
    latest_checkpoint_seq_number: Arc<AtomicU64>,
}

impl fmt::Debug for CheckpointProcessor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CheckpointProcessor")
            .field("stores", &self.stores)
            .field("package_store", &self.package_store)
            .field("original_system_pkg_id", &self.original_system_pkg_id)
            .finish()
    }
}

impl CheckpointProcessor {
    /// Creates a new instance of the checkpoint processor.
    pub fn new(
        stores: EventProcessorStores,
        package_store: LocalDBPackageStore,
        original_system_pkg_id: ObjectID,
    ) -> Self {
        Self {
            stores,
            package_store: package_store.clone(),
            original_system_pkg_id,
            package_resolver: Arc::new(Resolver::new(PackageCache::new(package_store.clone()))),
            latest_checkpoint_seq_number: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Verifies the given checkpoint with the given previous checkpoint. This method will verify
    /// that the checkpoint summary matches the content and that the checkpoint contents match the
    /// transactions.
    pub fn verify_checkpoint(
        &self,
        checkpoint: &CheckpointData,
        prev_checkpoint: VerifiedCheckpoint,
    ) -> Result<VerifiedCheckpoint> {
        let Some(committee) = self.stores.committee_store.get(&())? else {
            anyhow::bail!("No committee found in the committee store");
        };

        let verified_checkpoint = sui_storage::verify_checkpoint_with_committee(
            Arc::new(committee.clone()),
            &prev_checkpoint,
            checkpoint.checkpoint_summary.clone(),
        )
        .map_err(|checkpoint| {
            anyhow::anyhow!(
                "Failed to verify sui checkpoint: {}",
                checkpoint.sequence_number
            )
        })?;

        // Verify that checkpoint summary matches the content
        if verified_checkpoint.content_digest != *checkpoint.checkpoint_contents.digest() {
            anyhow::bail!("Checkpoint summary does not match the content");
        }

        // Verify that the checkpoint contents match the transactions
        for (digests, transaction) in checkpoint
            .checkpoint_contents
            .iter()
            .zip(checkpoint.transactions.iter())
        {
            if *transaction.transaction.digest() != digests.transaction {
                anyhow::bail!("Transaction digest does not match");
            }

            if transaction.effects.digest() != digests.effects {
                anyhow::bail!("Effects digest does not match");
            }

            if transaction.effects.events_digest().is_some() != transaction.events.is_some() {
                anyhow::bail!("Events digest and events are inconsistent");
            }

            if let Some((events_digest, events)) = transaction
                .effects
                .events_digest()
                .zip(transaction.events.as_ref())
            {
                if *events_digest != events.digest() {
                    anyhow::bail!("Events digest does not match");
                }
            }
        }

        Ok(verified_checkpoint)
    }

    /// Processes the checkpoint data.
    pub async fn process_checkpoint_data(
        &self,
        checkpoint: CheckpointData,
        verified_checkpoint: VerifiedCheckpoint,
        next_event_index: u64,
    ) -> Result<u64> {
        let mut next_event_index = next_event_index;
        let mut write_batch = self.stores.event_store.batch();
        let mut counter = 0;

        // Process each transaction in the checkpoint
        for tx in checkpoint.transactions.into_iter() {
            self.package_store.update_batch(&tx.output_objects)?;
            let tx_events = tx.events.unwrap_or_default();
            let original_package_ids: Vec<ObjectID> =
                try_join_all(tx_events.data.iter().map(|event| {
                    let pkg_address = event.type_.address;
                    self.package_store.get_original_package_id(pkg_address)
                }))
                .await?;
            for (seq, tx_event) in tx_events
                .data
                .into_iter()
                .zip(original_package_ids)
                // Filter out events that are not from the Walrus system package.
                .filter(|(_, original_id)| *original_id == self.original_system_pkg_id)
                .map(|(event, _)| event)
                .enumerate()
            {
                tracing::trace!(?tx_event, "event received");
                let move_type_layout = self
                    .package_resolver
                    .type_layout(move_core_types::language_storage::TypeTag::Struct(
                        Box::new(tx_event.type_.clone()),
                    ))
                    .await?;
                let move_datatype_layout = match move_type_layout {
                    MoveTypeLayout::Struct(s) => Some(MoveDatatypeLayout::Struct(s)),
                    MoveTypeLayout::Enum(e) => Some(MoveDatatypeLayout::Enum(e)),
                    _ => None,
                }
                .ok_or(anyhow!("Failed to get move datatype layout"))?;
                let sui_event = SuiEvent::try_from(
                    tx_event,
                    *tx.transaction.digest(),
                    seq as u64,
                    None,
                    move_datatype_layout,
                )?;
                let contract_event: ContractEvent = sui_event.try_into()?;
                let event_sequence_number = CheckpointEventPosition::new(
                    *checkpoint.checkpoint_summary.sequence_number(),
                    counter,
                );
                let walrus_event =
                    PositionedStreamEvent::new(contract_event, event_sequence_number);
                write_batch
                    .insert_batch(
                        &self.stores.event_store,
                        std::iter::once((next_event_index, walrus_event)),
                    )
                    .map_err(|e| anyhow!("Failed to insert event into event store: {}", e))?;
                counter += 1;
                next_event_index += 1;
            }
        }

        // Add checkpoint boundary event
        let end_of_checkpoint = PositionedStreamEvent::new_checkpoint_boundary(
            checkpoint.checkpoint_summary.sequence_number,
            counter,
        );
        write_batch.insert_batch(
            &self.stores.event_store,
            std::iter::once((next_event_index, end_of_checkpoint)),
        )?;

        next_event_index += 1;

        // Update committee if this is an end of epoch checkpoint
        if let Some(end_of_epoch_data) = &checkpoint.checkpoint_summary.end_of_epoch_data {
            let next_committee = end_of_epoch_data
                .next_epoch_committee
                .iter()
                .cloned()
                .collect();
            let committee = Committee::new(
                checkpoint.checkpoint_summary.epoch().saturating_add(1),
                next_committee,
            );
            write_batch.insert_batch(
                &self.stores.committee_store,
                std::iter::once(((), committee)),
            )?;
            self.package_resolver
                .package_store()
                .evict(SYSTEM_PACKAGE_ADDRESSES.iter().copied());
        }

        // Update checkpoint store
        write_batch.insert_batch(
            &self.stores.checkpoint_store,
            std::iter::once(((), verified_checkpoint.serializable_ref())),
        )?;

        // Write all changes
        write_batch.write()?;

        self.update_cached_latest_checkpoint_seq_number(*verified_checkpoint.sequence_number());

        Ok(next_event_index)
    }

    /// Updates the cached checkpoint sequence number.
    pub fn update_cached_latest_checkpoint_seq_number(&self, sequence_number: u64) {
        self.latest_checkpoint_seq_number
            .fetch_max(sequence_number, Ordering::SeqCst);
    }

    /// Gets the latest checkpoint sequence number, preferring the cache.
    pub fn get_latest_checkpoint_sequence_number(&self) -> Option<u64> {
        Some(self.latest_checkpoint_seq_number.load(Ordering::SeqCst))
    }
}
