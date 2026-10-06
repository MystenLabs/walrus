// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Bootstrapping a brand-new node's blob info tables from the latest certified blob info snapshot.
//!
//! A brand-new node (no persisted event cursor: a new operator or a wiped storage directory) whose
//! local event store does not reach back to the first event cannot rebuild its blob info by
//! replay. Replaying the partial history would miss pooled blobs registered before it, so instead
//! the node loads the latest certified snapshot and replays from the snapshot's epoch boundary.
//!
//! This runs while the node is being constructed, before any component that reads or writes the
//! blob info tables starts (event processing, blob and shard syncs, node recovery, garbage
//! collection, and the REST API). A node that is not brand-new, or whose replay is covered, is left
//! untouched.

use std::time::Duration;

use anyhow::{Context as _, bail};
use walrus_core::encoding::{ConsistencyCheckType, Primary};

use super::{
    contract_service::SystemContractService,
    storage::{Storage, blob_info_snapshot},
    system_events::EventManager,
};
use crate::{
    common::{config::SuiConfig, utils::create_walrus_client_with_refresher},
    event::events::EventStreamCursor,
};

/// How often, and how many times, reading the certified snapshot is attempted. A snapshot certified
/// moments before the node starts may not be readable yet: the storage nodes report it only once
/// they have processed its certification.
const READ_RETRY_DELAY: Duration = Duration::from_secs(10);
const READ_ATTEMPTS: u32 = 30;

/// Loads the latest certified blob info snapshot into a brand-new node whose event replay is not
/// covered by its local event store.
///
/// Does nothing if the node has processed events before, if the local event store starts at the
/// first event, or if `enabled` is false (the node then takes the incomplete-history path as
/// before). Fails if a snapshot is needed but none can be used, so that a brand-new node never
/// starts with partial blob info.
pub(super) async fn bootstrap_from_snapshot_if_needed(
    enabled: bool,
    storage: &Storage,
    event_manager: &dyn EventManager,
    contract_service: &dyn SystemContractService,
    sui_config: Option<&SuiConfig>,
) -> anyhow::Result<()> {
    let next_event_index = storage
        .get_event_cursor_and_next_index()?
        .map_or(0, |cursor| cursor.next_event_index());
    if next_event_index != 0 {
        return Ok(());
    }

    // Event-blob catch-up that reaches back to the first event also records an init state, at
    // index 0; the replay is then covered as well.
    let Some(init_state) = event_manager
        .init_state(EventStreamCursor::new(None, 0))
        .await?
        .filter(|init_state| init_state.event_cursor.element_index > 0)
    else {
        tracing::info!(
            "brand-new node with complete event history; replaying from the first event"
        );
        return Ok(());
    };
    let first_available_event_index = init_state.event_cursor.element_index;

    if !enabled {
        tracing::warn!(
            first_available_event_index,
            "brand-new node without complete event history and snapshot bootstrap disabled; \
            the node starts with incomplete history"
        );
        return Ok(());
    }

    let snapshot = contract_service
        .last_certified_snapshot_blob()
        .await
        .context("failed to read the latest certified blob info snapshot")?
        .context(
            "a brand-new node without complete event history needs a certified blob info \
            snapshot, but none is certified",
        )?;
    let (current_epoch, _) = contract_service.get_epoch_and_state().await?;
    if snapshot.end_epoch <= current_epoch {
        bail!(
            "the latest certified blob info snapshot (epoch {}, blob ID {}) expired at epoch {}",
            snapshot.epoch,
            snapshot.blob_id,
            snapshot.end_epoch
        );
    }

    let sui_config =
        sui_config.context("reading the certified blob info snapshot needs a Sui config")?;
    let walrus_client = create_walrus_client_with_refresher(
        sui_config.contract_config.clone(),
        sui_config.new_read_client().await?,
    )
    .await?;
    tracing::info!(
        snapshot_epoch = snapshot.epoch,
        blob_id = %snapshot.blob_id,
        first_available_event_index,
        "bootstrapping a brand-new node from the latest certified blob info snapshot"
    );
    let mut attempt = 1;
    let bytes = loop {
        match walrus_client
            .read_blob_with_consistency_check_type::<Primary>(
                &snapshot.blob_id,
                ConsistencyCheckType::Strict,
            )
            .await
        {
            Ok(bytes) => break bytes,
            Err(error) if attempt < READ_ATTEMPTS => {
                tracing::warn!(
                    ?error,
                    attempt,
                    blob_id = %snapshot.blob_id,
                    "failed to read the certified blob info snapshot; retrying"
                );
                attempt += 1;
                tokio::time::sleep(READ_RETRY_DELAY).await;
            }
            Err(error) => {
                return Err(error).with_context(|| {
                    format!(
                        "failed to read the certified blob info snapshot {} after {attempt} \
                        attempts",
                        snapshot.blob_id
                    )
                });
            }
        }
    };

    let (header, per_object, pooled, pools) = blob_info_snapshot::read_snapshot(&bytes)
        .context("failed to decode the certified blob info snapshot")?;
    if header.epoch != snapshot.epoch {
        bail!(
            "the certified blob info snapshot of epoch {} has epoch {} in its header",
            snapshot.epoch,
            header.epoch
        );
    }
    let boundary_event_index = header.event_cursor.next_event_index().saturating_sub(1);
    if boundary_event_index < first_available_event_index {
        bail!(
            "the certified blob info snapshot of epoch {} ends at event {boundary_event_index}, \
            before the first event the local event store holds ({first_available_event_index})",
            snapshot.epoch
        );
    }

    let aggregate_count = storage.load_blob_info_snapshot(&header, &per_object, &pooled, &pools)?;
    #[cfg(msim)]
    sui_macros::fail_point!("fail_point_blob_info_snapshot_bootstrap_loaded");
    tracing::info!(
        snapshot_epoch = snapshot.epoch,
        blob_id = %snapshot.blob_id,
        per_object_count = per_object.len(),
        pooled_count = pooled.len(),
        storage_pool_count = pools.len(),
        aggregate_count,
        boundary_event_index,
        "loaded the certified blob info snapshot"
    );
    Ok(())
}
