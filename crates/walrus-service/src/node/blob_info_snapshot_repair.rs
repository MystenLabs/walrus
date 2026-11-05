// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Temporary, testnet-only repair of a node's invalidated blob IDs from the certified snapshot.
//!
//! Testnet nodes that recovered with incomplete event history, or from a snapshot without
//! invalidated blob IDs, lack the marks of the blobs invalidated before their history starts, and
//! so write a different blob info snapshot than the rest of the committee. At startup, such a node
//! compares the snapshot it wrote at its last boundary with the certified one of the same epoch:
//! when the two differ only in the invalidated blob IDs, it takes the certified marks and deletes
//! the data of the newly marked blobs, after which it writes the certified bytes at the next
//! boundary.
//!
//! The repair runs only on testnet, only while the node's snapshots carry invalidated blob IDs,
//! and only before any component that uses the blob info tables starts. It never fails the
//! startup. Remove this module once every testnet node holds the certified marks.

use std::path::Path;

use anyhow::Context as _;

use super::{
    blob_info_snapshot_bootstrap::read_certified_snapshot_bytes,
    blob_info_snapshot_writer::{self, BlobInfoSnapshotWriterConfig},
    contract_service::SystemContractService,
    network_overrides::{NetworkKind, network_kind_for_contracts},
    storage::{Storage, blob_info_snapshot},
};
use crate::common::config::SuiConfig;

/// How many times reading the certified snapshot is attempted; the node starts without the repair
/// after that.
const READ_ATTEMPTS: u32 = 3;

/// Repairs the node's invalidated blob IDs from the certified blob info snapshot when the node's
/// own snapshot of the same epoch differs from it only in them (see the module docs). Errors are
/// logged, and the node starts regardless.
pub(super) async fn repair_invalid_blobs_if_needed(
    config: &BlobInfoSnapshotWriterConfig,
    storage_path: &Path,
    storage: &Storage,
    contract_service: &dyn SystemContractService,
    sui_config: Option<&SuiConfig>,
) {
    if let Err(error) =
        try_repair_invalid_blobs(config, storage_path, storage, contract_service, sui_config).await
    {
        tracing::warn!(
            ?error,
            "failed to repair the invalidated blob IDs from the certified blob info snapshot; \
            starting without the repair"
        );
    }
}

async fn try_repair_invalid_blobs(
    config: &BlobInfoSnapshotWriterConfig,
    storage_path: &Path,
    storage: &Storage,
    contract_service: &dyn SystemContractService,
    sui_config: Option<&SuiConfig>,
) -> anyhow::Result<()> {
    let Some(sui_config) = sui_config else {
        return Ok(());
    };
    let contract_config = &sui_config.contract_config;
    if network_kind_for_contracts(
        contract_config.system_object,
        contract_config.staking_object,
    ) != NetworkKind::Testnet
    {
        return Ok(());
    }
    if !config.enabled || config.include_invalid_blobs_from_epoch.is_none() {
        return Ok(());
    }

    let base_dir = blob_info_snapshot_writer::snapshot_base_dir(storage_path);
    let Some((own_epoch, own_path)) = blob_info_snapshot_writer::latest_snapshot_file(&base_dir)
    else {
        tracing::info!("no own blob info snapshot file; no invalidated blob IDs to repair");
        return Ok(());
    };
    let Some(certified) = contract_service
        .last_certified_snapshot_blob()
        .await
        .context("failed to read the latest certified blob info snapshot")?
    else {
        tracing::info!("no certified blob info snapshot; no invalidated blob IDs to repair");
        return Ok(());
    };
    if certified.epoch != own_epoch {
        tracing::info!(
            certified_epoch = certified.epoch,
            own_epoch,
            "the certified blob info snapshot is not of the epoch of the own snapshot; nothing \
            to compare the invalidated blob IDs with"
        );
        return Ok(());
    }
    if storage.snapshot_publication()?.is_some_and(|publication| {
        publication.epoch() == own_epoch && publication.blob_id() == certified.blob_id
    }) {
        tracing::info!(
            epoch = own_epoch,
            blob_id = %certified.blob_id,
            "this node published the certified blob info snapshot; no invalidated blob IDs to \
            repair"
        );
        return Ok(());
    }

    let own_bytes = std::fs::read(&own_path)
        .with_context(|| format!("failed to read the own snapshot {}", own_path.display()))?;
    let own = blob_info_snapshot::read_snapshot(&own_bytes)
        .context("failed to decode the own blob info snapshot")?;
    if own.invalid_blobs.is_none() {
        tracing::info!(
            epoch = own_epoch,
            format_version = own.format.version(),
            "the own blob info snapshot carries no invalidated blob IDs; nothing to repair"
        );
        return Ok(());
    }
    let certified_bytes = read_certified_snapshot_bytes(&certified, sui_config, READ_ATTEMPTS)
        .await
        .context("failed to read the certified blob info snapshot")?;
    let certified_snapshot = blob_info_snapshot::read_snapshot(&certified_bytes)
        .context("failed to decode the certified blob info snapshot")?;

    let repair = storage
        .repair_invalid_blobs_from_certified_snapshot(&own, &certified_snapshot)
        .context(
            "the own and the certified blob info snapshots differ beyond the invalidated \
        blob IDs; not repaired",
        )?;
    tracing::info!(
        epoch = own_epoch,
        blob_id = %certified.blob_id,
        added = repair.added,
        replaced = repair.replaced,
        extra = repair.extra,
        "repaired the invalidated blob IDs from the certified blob info snapshot"
    );
    Ok(())
}
