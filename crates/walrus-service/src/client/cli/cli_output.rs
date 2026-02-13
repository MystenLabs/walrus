// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{HashMap, HashSet},
    io::{Write, stdout},
    num::NonZeroU16,
    path::PathBuf,
};

use anyhow::Result;
use colored::Colorize;
use indoc::printdoc;
use itertools::Itertools as _;
use prettytable::{Table, format, row};
use serde::Serialize;
use walrus_core::{
    BlobId,
    ShardIndex,
    metadata::{QuiltMetadata, QuiltMetadataV1},
};
use walrus_sdk::{
    client::{
        client_types::{self, StoredQuiltPatch},
        resource::RegisterBlobOp,
        responses::{BlobStoreResult, BlobStoreResultWithPath, QuiltStoreResult},
    },
    format_event_id,
};
use walrus_storage_node_client::api::{BlobStatus, DeletableCounts, EventProgress};
use walrus_sui::types::Blob;

use crate::client::{
    cli::{
        HumanReadableBytes,
        HumanReadableFrost,
        HumanReadableMist,
        WalrusColors,
        error,
        success,
        thousands_separator,
        warning,
    },
    responses::{
        BlobIdConversionOutput,
        BlobIdOutput,
        BlobStatusOutput,
        DeleteOutput,
        DryRunOutput,
        EncodingDependentPriceInfo,
        EpochTimeOrMessage,
        ExampleBlobInfo,
        ExchangeOutput,
        ExtendBlobOutput,
        FundSharedBlobOutput,
        GetBlobAttributeOutput,
        InfoBftOutput,
        InfoCommitteeOutput,
        InfoEpochOutput,
        InfoOutput,
        InfoPriceOutput,
        InfoSizeOutput,
        InfoStorageOutput,
        NodeHealthOutput,
        ReadOutput,
        ReadQuiltOutput,
        ServiceHealthInfoOutput,
        ShareBlobOutput,
        StakeOutput,
        StorageNodeInfo,
        StoreQuiltDryRunOutput,
        WalletOutput,
    },
};

/// Trait to differentiate output depending on the output mode.
pub trait CliOutput: Serialize {
    /// Writes the output to stdout as a human-readable string for the CLI.
    fn print_cli_output(&self);

    /// Writes the output to stdout formatted depending on the output mode.
    fn print_output(&self, json: bool) -> Result<()> {
        if json {
            serde_json::to_writer_pretty(stdout(), &self)?;
        } else {
            self.print_cli_output();
        }
        Ok(())
    }
}
impl CliOutput for Vec<BlobStoreResultWithPath> {
    fn print_cli_output(&self) {
        for result in self {
            result.print_cli_output();
        }

        let mut total_encoded_size = 0;
        let mut total_cost = 0;
        let mut reuse_and_extend_count = 0;
        let mut newly_certified = 0;

        for res in self.iter() {
            if let BlobStoreResult::NewlyCreated {
                resource_operation,
                cost,
                ..
            } = &res.blob_store_result
            {
                total_encoded_size += resource_operation.encoded_length();
                total_cost += cost;
                match resource_operation {
                    RegisterBlobOp::ReuseAndExtend { .. } => {
                        reuse_and_extend_count += 1;
                    }
                    RegisterBlobOp::RegisterFromScratch { .. }
                    | RegisterBlobOp::ReuseAndExtendNonCertified { .. }
                    | RegisterBlobOp::ReuseStorage { .. }
                    | RegisterBlobOp::ReuseRegistration { .. } => {
                        newly_certified += 1;
                    }
                }
            }
        }

        let mut parts = Vec::new();
        if newly_certified > 0 {
            parts.push(format!("{newly_certified} newly certified"));
        }
        if reuse_and_extend_count > 0 {
            parts.push(format!("{reuse_and_extend_count} extended"));
        }

        if !parts.is_empty() {
            println!(
                "{} ({})",
                "Summary for Modified or Created Blobs"
                    .bold()
                    .walrus_purple(),
                parts.join(", ")
            );
            println!(
                "Total encoded size: {}",
                HumanReadableBytes(total_encoded_size)
            );
            println!("Total cost: {}", HumanReadableFrost::from(total_cost));
        } else {
            println!(
                "{}",
                "No blobs were modified or created".bold().walrus_purple()
            );
        }
    }
}

impl CliOutput for Vec<DryRunOutput> {
    fn print_cli_output(&self) {
        for result in self {
            result.print_cli_output();
        }
    }
}

impl CliOutput for BlobStoreResultWithPath {
    fn print_cli_output(&self) {
        match &self.blob_store_result {
            BlobStoreResult::AlreadyCertified {
                blob_id,
                event_or_object,
                end_epoch,
            } => {
                println!(
                    "{} Blob was already available and certified within Walrus, \
                    for a sufficient number of epochs.\nPath: {}\n\
                    Blob ID: {}\n{event_or_object}\nExpiry epoch (exclusive): {}\n",
                    success(),
                    self.path.display(),
                    blob_id,
                    end_epoch,
                )
            }
            BlobStoreResult::NewlyCreated {
                blob_object,
                resource_operation,
                cost,
                shared_blob_object,
            } => {
                let operation_str = match resource_operation {
                    RegisterBlobOp::RegisterFromScratch { .. } => {
                        "(storage was purchased, and a new blob object was registered)"
                    }
                    RegisterBlobOp::ReuseStorage { .. } => {
                        "(already-owned storage was reused, and a new blob object was registered)"
                    }
                    RegisterBlobOp::ReuseRegistration { .. } => {
                        "(an existing registration was reused)"
                    }
                    RegisterBlobOp::ReuseAndExtend { .. } => "(the blob was extended in lifetime)",
                    RegisterBlobOp::ReuseAndExtendNonCertified { .. } => {
                        "(an existing registration was reused and extended)"
                    }
                };
                println!(
                    "{} {} blob stored successfully.\n\
                    Path: {}\n\
                    Blob ID: {}\n\
                    Sui object ID: {}\n\
                    Unencoded size: {}\n\
                    Encoded size (including replicated metadata): {}\n\
                    Cost (excluding gas): {} {} \n\
                    Expiry epoch (exclusive): {}{}\n\
                    Encoding type: {}\n",
                    success(),
                    if blob_object.deletable {
                        "Deletable"
                    } else {
                        "Permanent"
                    },
                    self.path.display(),
                    blob_object.blob_id,
                    blob_object.id,
                    HumanReadableBytes(blob_object.size),
                    HumanReadableBytes(resource_operation.encoded_length()),
                    HumanReadableFrost::from(*cost),
                    operation_str,
                    blob_object.storage.end_epoch,
                    shared_blob_object
                        .map(|id| format!("\nShared blob object ID: {id}"))
                        .unwrap_or_default(),
                    blob_object.encoding_type,
                )
            }
            BlobStoreResult::MarkedInvalid { blob_id, event } => {
                println!(
                    "{} Blob was marked as invalid.\nPath: {}\nBlob ID: {}\n
                    Invalidation event ID: {}\n",
                    error(),
                    self.path.display(),
                    blob_id,
                    format_event_id(event),
                )
            }
            BlobStoreResult::Error {
                blob_id,
                error_msg,
                failure_phase,
            } => {
                println!(
                    "{} Storing blob failed during {}.\nPath: {}\nBlob ID: {}\nError: {}",
                    error(),
                    failure_phase,
                    self.path.display(),
                    blob_id.map_or("unknown".to_string(), |id| id.to_string()),
                    error_msg,
                )
            }
        }
    }
}

impl CliOutput for QuiltStoreResult {
    fn print_cli_output(&self) {
        let blob_store_result = BlobStoreResultWithPath {
            blob_store_result: self.blob_store_result.clone(),
            path: PathBuf::from("path(s) ignored for quilt store result"),
        };
        blob_store_result.print_cli_output();

        let table_output = construct_stored_quilt_patch_table(&self.stored_quilt_blobs);
        table_output.printstd();
    }
}

impl CliOutput for ReadOutput {
    fn print_cli_output(&self) {
        if let Some(path) = &self.out {
            println!(
                "{} Blob {} reconstructed from Walrus and written to {}.",
                success(),
                self.blob_id,
                path.display()
            )
        }
    }
}

impl CliOutput for BlobIdOutput {
    fn print_cli_output(&self) {
        println!(
            "{} Blob from file '{}' encoded successfully.\n\
                Unencoded size: {}\n\
                Blob ID: {}\n\
                Encoding type: {}",
            success(),
            self.file.display(),
            self.unencoded_length,
            self.blob_id,
            self.encoding_type,
        )
    }
}

impl CliOutput for DryRunOutput {
    fn print_cli_output(&self) {
        println!(
            "{} Store dry-run succeeded.\n\
                Path: {}\n\
                Blob ID: {}\n\
                Encoding type: {}\n\
                Unencoded size: {}\n\
                Encoded size (including replicated metadata): {}\n\
                Cost to store as new blob (excluding gas): {}\n",
            success(),
            self.path.display(),
            self.blob_id,
            self.encoding_type,
            HumanReadableBytes(self.unencoded_size),
            HumanReadableBytes(self.encoded_size),
            HumanReadableFrost::from(self.storage_cost),
        )
    }
}

impl CliOutput for StoreQuiltDryRunOutput {
    fn print_cli_output(&self) {
        self.quilt_blob_output.print_cli_output();
        construct_stored_quilt_patch_table(&client_types::get_stored_quilt_patches(
            &self.quilt_index,
            self.quilt_blob_output.blob_id,
        ))
        .printstd();
    }
}

fn construct_stored_quilt_patch_table(quilt_patches: &[StoredQuiltPatch]) -> Table {
    let mut table = Table::new();
    table.set_format(default_table_format());
    table.set_titles(row![
        b->"Index",
        b->"QuiltPatchId",
        b->"Sliver Range",
        b->"Identifier",
    ]);

    for (i, quilt_patch) in quilt_patches.iter().enumerate() {
        table.add_row(row![
            bFc->format!("{i}"),
            quilt_patch.quilt_patch_id,
            quilt_patch
                .range
                .map(|range| format!("[{}, {})", range.0, range.1))
                .unwrap_or_default(),
            quilt_patch.identifier,
        ]);
    }

    table
}

impl CliOutput for BlobStatusOutput {
    fn print_cli_output(&self) {
        let blob_str = blob_and_file_str(&self.blob_id, &self.file);
        let expiry_str = self
            .estimated_expiry_timestamp
            .map(|expiry| format!("Estimated expiry timestamp: {}\n", expiry.to_rfc3339()))
            .unwrap_or_default();
        match self.status {
            BlobStatus::Nonexistent => println!("Blob ID {blob_str} is not stored on Walrus."),
            BlobStatus::Deletable {
                initial_certified_epoch,
                deletable_counts:
                    DeletableCounts {
                        count_deletable_total,
                        count_deletable_certified,
                    },
            } => {
                let initial_certified_str = initial_certified_epoch
                    .map(|epoch| format!(", initially certified in epoch {epoch}"))
                    .unwrap_or_default();
                println!(
                    "Blob ID {blob_str} is registered on Walrus, but only in one or more \
                    deletable Blob objects:\n\
                    Total number of certified objects: {count_deletable_certified} (of \
                    {count_deletable_total} registered{initial_certified_str})"
                )
            }
            BlobStatus::Invalid { .. } => println!("Blob ID {blob_str} is invalid."),
            BlobStatus::Permanent {
                end_epoch,
                is_certified,
                status_event,
                initial_certified_epoch,
                deletable_counts:
                    DeletableCounts {
                        count_deletable_certified,
                        ..
                    },
            } => {
                let status = (if is_certified {
                    "certified"
                } else {
                    "registered"
                })
                .bold();
                let initial_certified_str = initial_certified_epoch
                    .map(|epoch| format!("\nInitially certified in epoch: {epoch}"))
                    .unwrap_or_default();
                println!(
                    "There is a {status} permanent Blob object for blob ID {blob_str}.\n\
                        Expiry epoch: {end_epoch}\n\
                        {expiry_str}\
                        Related event: {}\
                        {initial_certified_str}",
                    format_event_id(&status_event)
                );
                match count_deletable_certified {
                    2.. => {
                        println!(
                            "There are also {count_deletable_certified} certified deletable Blob \
                            objects for this blob ID."
                        )
                    }
                    1 => {
                        println!(
                            "There is also 1 certified deletable Blob object for this blob ID."
                        )
                    }
                    0 => {}
                }
            }
        }
    }
}

impl CliOutput for BlobIdConversionOutput {
    fn print_cli_output(&self) {
        println!("Walrus blob ID: {}", self.0);
    }
}

impl CliOutput for InfoOutput {
    fn print_cli_output(&self) {
        let Self {
            epoch_info,
            storage_info,
            size_info,
            price_info,
            committee_info,
            bft_info,
        } = self;

        // NOTE: keep text in sync with changes in the contracts.
        println!("\n{}", "Walrus system information".bold().walrus_purple());

        // Print epoch info.
        epoch_info.print_cli_output();

        // Print storage info.
        storage_info.print_cli_output();

        // Print size info
        size_info.print_cli_output();

        // Print price info.
        price_info.print_cli_output();

        // Print BFT info
        if let Some(bft_info) = bft_info {
            bft_info.print_cli_output();
        }

        // Print committee info.
        if let Some(committee_info) = committee_info {
            committee_info.print_cli_output();
        }
    }
}

impl CliOutput for InfoEpochOutput {
    fn print_cli_output(&self) {
        let Self {
            current_epoch,
            start_of_current_epoch,
            epoch_duration,
            max_epochs_ahead,
        } = self;

        let time_output = match start_of_current_epoch {
            EpochTimeOrMessage::DateTime(start_time) => {
                let end_time = *start_time
                    + chrono::Duration::from_std(*epoch_duration)
                        .expect("any practical epoch duration fits into a chrono::Duration");
                format!("Start time: {start_time}\nEnd time: {end_time}")
            }
            EpochTimeOrMessage::Message(msg) => msg.clone(),
        };

        printdoc!(
            "

            {heading}
            Current epoch: {current_epoch}
            {time_output}
            Epoch duration: {hr_epoch_duration}
            Blobs can be stored for at most {max_epochs_ahead} epochs in the future.
            ",
            heading = "Epochs and storage duration".bold().walrus_teal(),
            hr_epoch_duration = humantime::format_duration(*epoch_duration),
        );
    }
}

impl CliOutput for InfoStorageOutput {
    fn print_cli_output(&self) {
        let Self { n_shards, n_nodes } = self;

        printdoc!(
            "

            {heading}
            Number of storage nodes: {n_nodes}
            Number of shards: {n_shards}
            ",
            heading = "Storage nodes".bold().walrus_teal(),
        );
    }
}

impl CliOutput for InfoSizeOutput {
    fn print_cli_output(&self) {
        let Self {
            storage_unit_size: unit_size,
            max_blob_size,
        } = self;

        printdoc!(
            "

            {heading}
            Maximum blob size: {hr_max_blob} ({max_blob_size_sep} B)
            Storage unit: {hr_storage_unit}
            ",
            heading = "Blob size".bold().walrus_teal(),
            hr_max_blob = HumanReadableBytes(*max_blob_size),
            hr_storage_unit = HumanReadableBytes(*unit_size),
            max_blob_size_sep = thousands_separator(*max_blob_size),
        );
    }
}

impl CliOutput for InfoPriceOutput {
    fn print_cli_output(&self) {
        let Self {
            storage_price_per_unit_size,
            write_price_per_unit_size,
            encoding_dependent_price_info,
        } = self;

        printdoc!(
            "

            {price_heading}
            (Conversion rate: 1 WAL = 1,000,000,000 FROST)
            Price per encoded storage unit: {hr_storage_price_per_unit_size}
            Additional price for each write: {hr_write_price_per_unit_size}
            ",
            price_heading = "Storage prices per epoch".bold().walrus_teal(),
            hr_storage_price_per_unit_size = HumanReadableFrost::from(*storage_price_per_unit_size),
            hr_write_price_per_unit_size = HumanReadableFrost::from(*write_price_per_unit_size),
        );

        for encoding_type in encoding_dependent_price_info {
            encoding_type.print_cli_output();
        }
    }
}

impl CliOutput for EncodingDependentPriceInfo {
    fn print_cli_output(&self) {
        let Self {
            marginal_size,
            metadata_price,
            marginal_price,
            example_blobs,
            encoding_type,
        } = self;

        printdoc!(
            "

            {price_heading}
            Price to store metadata: {metadata_price}
            Marginal price per additional {marginal_size:.0} (w/o metadata): {marginal_price}
            Total price for example blob sizes:
            {example_blob_output}
            ",
            price_heading =
                format!("Approximate prices to store with the {encoding_type} encoding")
                    .bold()
                    .walrus_teal(),
            metadata_price = HumanReadableFrost::from(*metadata_price),
            marginal_size = HumanReadableBytes(*marginal_size),
            marginal_price = HumanReadableFrost::from(*marginal_price),
            example_blob_output = example_blobs
                .iter()
                .map(ExampleBlobInfo::cli_output)
                .collect::<Vec<_>>()
                .join("\n"),
        );
    }
}

impl CliOutput for InfoCommitteeOutput {
    fn print_cli_output(&self) {
        let Self {
            n_shards,
            n_primary_source_symbols,
            n_secondary_source_symbols,
            metadata_storage_size,
            max_sliver_size,
            max_encoded_blob_size,
            current_storage_nodes,
            next_storage_nodes,
            print_details,
        } = self;

        printdoc!(
            "

            {encoding_heading}
            Number of shards: {n_shards}
            Number of primary source symbols: {n_primary_source_symbols}
            Number of secondary source symbols: {n_secondary_source_symbols}
            Metadata size: {hr_metadata} ({metadata_storage_size_sep} B)
            Maximum sliver size: {hr_sliver} ({max_sliver_size_sep} B)
            Maximum encoded blob size: {hr_encoded} ({max_encoded_blob_size_sep} B)

            {node_heading}
            ",
            encoding_heading = "Encoding parameters and sizes".bold().walrus_purple(),
            hr_metadata = HumanReadableBytes(*metadata_storage_size),
            metadata_storage_size_sep = thousands_separator(*metadata_storage_size),
            hr_sliver = HumanReadableBytes(*max_sliver_size),
            max_sliver_size_sep = thousands_separator(*max_sliver_size),
            hr_encoded = HumanReadableBytes(*max_encoded_blob_size),
            max_encoded_blob_size_sep = thousands_separator(*max_encoded_blob_size),
            node_heading = "Current committee".bold().walrus_purple()
        );

        print_storage_node_info(n_shards, current_storage_nodes, *print_details);

        if next_storage_nodes.is_empty() {
            return;
        }

        println!("\n{}", "Next committee".bold().walrus_purple());
        print_storage_node_info(n_shards, next_storage_nodes, *print_details);

        print_committee_changes(current_storage_nodes, next_storage_nodes);
    }
}

fn print_committee_changes(
    current_storage_nodes: &[StorageNodeInfo],
    next_storage_nodes: &[StorageNodeInfo],
) {
    // Detect changes between current and next committee
    let current_node_ids: HashSet<_> = current_storage_nodes.iter().map(|n| n.node_id).collect();
    let next_node_ids: HashSet<_> = next_storage_nodes.iter().map(|n| n.node_id).collect();

    let onboarding: Vec<_> = next_storage_nodes
        .iter()
        .filter(|n| !current_node_ids.contains(&n.node_id))
        .collect();

    let offboarding: Vec<_> = current_storage_nodes
        .iter()
        .filter(|n| !next_node_ids.contains(&n.node_id))
        .collect();

    let next_storage_nodes: HashMap<_, _> = next_storage_nodes
        .iter()
        .map(|n| (n.node_id, n.clone()))
        .collect();
    let mut nodes_with_shards_gained: Vec<(&StorageNodeInfo, Vec<ShardIndex>)> = Vec::new();
    let mut nodes_with_shards_lost: Vec<(&StorageNodeInfo, Vec<ShardIndex>)> = Vec::new();
    for node_info in current_storage_nodes.iter() {
        let Some(node_next_epoch_info) = next_storage_nodes.get(&node_info.node_id) else {
            continue;
        };
        let current_shards: HashSet<_> = node_info.shard_ids.iter().collect();
        let next_shards: HashSet<_> = node_next_epoch_info.shard_ids.iter().collect();

        let shards_lost: Vec<ShardIndex> = current_shards
            .difference(&next_shards)
            .map(|s| **s)
            .collect();
        let shards_gained: Vec<ShardIndex> = next_shards
            .difference(&current_shards)
            .map(|s| **s)
            .collect();

        if !shards_lost.is_empty() {
            nodes_with_shards_lost.push((node_info, shards_lost));
        }
        if !shards_gained.is_empty() {
            nodes_with_shards_gained.push((node_info, shards_gained));
        }
    }

    // Print changes section if there are any and details are enabled
    if onboarding.is_empty()
        && offboarding.is_empty()
        && nodes_with_shards_gained.is_empty()
        && nodes_with_shards_lost.is_empty()
    {
        println!(
            "\n{}",
            "Committee members and shard distribution remain the same for next epoch."
                .bold()
                .walrus_purple()
        );
        return;
    }
    println!(
        "\n{}",
        "Committee changes for next epoch".bold().walrus_purple()
    );

    if !onboarding.is_empty() {
        println!(
            "\n  {} ({}):",
            "Nodes onboarding".bold().walrus_teal(),
            onboarding.len()
        );
        for node in &onboarding {
            println!(
                "    • {} (ID: {}) - {} shard{s}: {}",
                node.name,
                node.node_id,
                node.n_shards,
                print_list(&node.shard_ids),
                s = plural_ending(&node.shard_ids),
            );
        }
    }

    if !offboarding.is_empty() {
        println!(
            "\n  {} ({}):",
            "Nodes offboarding".bold().walrus_teal(),
            offboarding.len()
        );
        for node in &offboarding {
            println!(
                "    • {} (ID: {}) - {} shard{s}: {}",
                node.name,
                node.node_id,
                node.n_shards,
                print_list(&node.shard_ids),
                s = plural_ending(&node.shard_ids),
            );
        }
    }

    if !nodes_with_shards_gained.is_empty() {
        println!(
            "\n  {} ({} node{s}):",
            "Nodes with shards gained".bold().walrus_teal(),
            nodes_with_shards_gained.len(),
            s = plural_ending(&nodes_with_shards_gained),
        );
        for (node, shards_gained) in &nodes_with_shards_gained {
            println!(
                "    • {} (ID: {}) - gaining shard{s}: {}",
                node.name,
                node.node_id,
                print_list(shards_gained),
                s = plural_ending(shards_gained),
            );
        }
    }

    if !nodes_with_shards_lost.is_empty() {
        println!(
            "\n  {} ({} node{s}):",
            "Nodes with shards lost".bold().walrus_teal(),
            nodes_with_shards_lost.len(),
            s = plural_ending(&nodes_with_shards_lost),
        );
        for (node, shards_lost) in &nodes_with_shards_lost {
            println!(
                "    • {} (ID: {}) - losing shard{s}: {}",
                node.name,
                node.node_id,
                print_list(shards_lost),
                s = plural_ending(shards_lost),
            );
        }
    }
}

impl CliOutput for InfoBftOutput {
    fn print_cli_output(&self) {
        let Self {
            max_faulty_shards,
            quorum_threshold,
            min_correct_shards,
            min_nodes_above,
            shards_above,
        } = self;

        printdoc!(
            "

            {heading}
            Tolerated faults (f): {max_faulty_shards}
            Quorum threshold (2f+1): {quorum_threshold}
            Minimum number of correct shards (n-f): {min_correct_shards}
            Minimum number of nodes to get above f: {min_nodes_above} ({shards_above} shards)
            ",
            heading = "BFT system information".bold().walrus_purple(),
            max_faulty_shards = max_faulty_shards,
            quorum_threshold = quorum_threshold,
            min_correct_shards = min_correct_shards,
            min_nodes_above = min_nodes_above,
            shards_above = shards_above,
        );
    }
}

fn print_storage_node_info(
    n_shards: &NonZeroU16,
    storage_nodes: &[StorageNodeInfo],
    print_details: bool,
) {
    let mut table = Table::new();
    table.set_format(default_table_format());
    table.set_titles(row![
        b->"Idx",
        b->"Name",
        b->"# Shards",
        b->"Stake",
        b->"Address",
    ]);
    for (i, node) in storage_nodes.iter().enumerate() {
        let n_owned = node.n_shards;
        let n_owned_percent = (n_owned as f64) / f64::from(n_shards.get()) * 100.0;
        table.add_row(row![
            bFc->format!("{i}"),
            node.name,
            r->format!("{} ({:.2}%)", n_owned, n_owned_percent),
            r->HumanReadableFrost::from(node.stake),
            node.network_address,
        ]);
    }
    table.printstd();
    if print_details {
        println!(
            "\n{}",
            "Storage node details and shard distribution"
                .bold()
                .walrus_purple(),
        );
        for (i, node) in storage_nodes.iter().enumerate() {
            print_single_storage_node_info(node, i, n_shards);
        }
    }
}

struct DisplayShardList<'a>(&'a [ShardIndex]);

impl std::fmt::Display for DisplayShardList<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let shard_ids = &self.0;
        if shard_ids.is_empty() {
            write!(f, "none")
        } else {
            let mut counter = 0;
            for (i, shard_id) in shard_ids.iter().enumerate() {
                if i > 0 && counter != 0 {
                    write!(f, ", ")?;
                }
                write!(f, "{}", usize::from(*shard_id))?;
                counter += 1;
                // Insert a newline after every 10 shard IDs for better readability
                if counter == 10 {
                    writeln!(f)?;
                    counter = 0;
                }
            }
            Ok(())
        }
    }
}

impl CliOutput for Vec<Blob> {
    fn print_cli_output(&self) {
        let mut table = Table::new();
        table.set_format(default_table_format());
        table.set_titles(row![
            b->"Blob ID",
            bc->"Unencoded size",
            bc->"Certified?",
            bc->"Deletable?",
            bc->"Exp. epoch",
            b->"Object ID",
        ]);

        for blob in self {
            table.add_row(row![
                blob.blob_id,
                c->HumanReadableBytes(blob.size),
                c->blob.certified_epoch.is_some(),
                c->blob.deletable,
                c->blob.storage.end_epoch,
                blob.id,
            ]);
        }
        table.printstd();
    }
}

impl CliOutput for DeleteOutput {
    fn print_cli_output(&self) {
        let identity = self.blob_identity.to_string();
        println!(
            "\n{}{}",
            "Delete result for: ".bold().walrus_purple(),
            identity
        );

        if let Some(error_msg) = &self.error {
            println!("{} Error: {}", error(), error_msg);
        } else if self.aborted {
            println!("{} Operation aborted. No blobs were deleted.", warning());
        } else if self.no_blob_found {
            println!("{} No owned deletable blobs found.", warning());
            return;
        }

        if !self.deleted_blobs.is_empty() {
            println!("{} The following objects were deleted:", success());
            self.deleted_blobs.print_cli_output();
            if let Some(post_deletion_status) = &self.post_deletion_status {
                let status_output = removed_instance_string(post_deletion_status);
                println!("{} {}", "Note:".bold().walrus_purple(), status_output);
            }
        }
    }
}

fn removed_instance_string(blob_status: &BlobStatus) -> String {
    const STILL_AVAILABLE: &str =
        "The above blob objects were removed, but the blob still exist on Walrus.";
    match blob_status {
        BlobStatus::Nonexistent => {
            "The blob was removed from Walrus (note that the data may still be publicly \
            available, i.e., if someone has downloaded it before deletion)."
                .to_owned()
        }
        BlobStatus::Invalid { .. } => "The blob was marked as invalid.".to_owned(),
        BlobStatus::Permanent {
            is_certified,
            deletable_counts,
            ..
        } => {
            format!(
                "{} There are still one or more {} permanent instances {}available.",
                STILL_AVAILABLE,
                is_certified.then_some("certified").unwrap_or("registered"),
                if deletable_counts.count_deletable_total > 0 {
                    format!(
                        "and deletable instances ({}) ",
                        deletable_counts_summary(deletable_counts)
                    )
                } else {
                    String::new()
                }
            )
        }
        BlobStatus::Deletable {
            deletable_counts, ..
        } => {
            format!(
                "{} There are still one or more deletable instances ({}) available.",
                STILL_AVAILABLE,
                deletable_counts_summary(deletable_counts)
            )
        }
    }
}

fn deletable_counts_summary(counts: &DeletableCounts) -> String {
    format!(
        "{} total, of which {} certified",
        counts.count_deletable_total, counts.count_deletable_certified
    )
}

impl CliOutput for StakeOutput {
    fn print_cli_output(&self) {
        let Some(first_wal) = self.staked_wal.first() else {
            println!("{} No WAL was staked.", warning());
            return;
        };
        if self.staked_wal.len() == 1 {
            println!("{} Staked WAL successfully:\n{}", success(), first_wal);
        } else {
            println!(
                "{} Staked WAL successfully on {} storage nodes:\n{}",
                success(),
                self.staked_wal.len(),
                self.staked_wal.iter().map(ToString::to_string).join("\n")
            );
        }
    }
}

impl CliOutput for WalletOutput {
    fn print_cli_output(&self) {
        println!(
            "{} Generated a new Sui wallet with address {}",
            success(),
            self.wallet_address
        );
    }
}

impl CliOutput for ExchangeOutput {
    fn print_cli_output(&self) {
        println!(
            "{} Exchanged {} for WAL.",
            success(),
            HumanReadableMist::from(self.amount_sui),
        );
    }
}

impl CliOutput for ShareBlobOutput {
    fn print_cli_output(&self) {
        println!(
            "{} The blob has been shared, object id: {} {}",
            success(),
            self.shared_blob_object_id,
            self.amount
                .map(|amount| format!(", funded with {}", HumanReadableFrost::from(amount)))
                .unwrap_or_default()
        );
    }
}

impl CliOutput for FundSharedBlobOutput {
    fn print_cli_output(&self) {
        println!(
            "{} The blob has been funded with {}",
            success(),
            HumanReadableFrost::from(self.amount)
        );
    }
}

impl CliOutput for ExtendBlobOutput {
    fn print_cli_output(&self) {
        println!(
            "{} The blob has been extended by {} epochs",
            success(),
            self.epochs_extended
        );
    }
}

impl NodeHealthOutput {
    fn print_cli_output(&self, latest_seq: Option<u64>) {
        printdoc! {"

            {heading}
            Node ID: {node_id}
            Node URL: {node_url}
            Network public key: {network_public_key}
            ",
            heading = self.node_name.bold().walrus_purple(),
            node_id = self.node_id,
            node_url = self.node_url,
            network_public_key = self.network_public_key,
        };
        match &self.health_info {
            Err(error) => {
                // Print detailed (debug) error message.
                println!("Error: {error:?}");
            }
            Ok(health_info) => {
                let EventProgress {
                    persisted,
                    pending,
                    highest_finished_event_index,
                } = health_info.event_progress;
                printdoc! {"

                    {general_heading}
                    Uptime: {uptime}
                    Current epoch: {epoch}
                    Public key: {public_key}
                    Node status: {node_status}

                    {event_heading}
                    Events persisted: {persisted}
                    Events pending: {pending}{highest_finished_event_index_output}

                    {checkpoint_heading}
                    {checkpoint_info}

                    {shard_heading}
                    Owned shards: {owned}
                    Read-only shards: {read_only}

                    {owned_status_heading}
                    Unknown: {unknown}
                    Ready: {ready}
                    In transfer: {in_transfer}
                    In recovery: {in_recovery}
                    ",
                    general_heading = "General Information".bold().walrus_teal(),
                    uptime = humantime::format_duration(
                        std::time::Duration::from_secs(health_info.uptime.as_secs())
                    ),
                    epoch = health_info.epoch,
                    public_key = health_info.public_key,
                    node_status = health_info.node_status,
                    event_heading = "Event Progress".bold().walrus_teal(),
                    highest_finished_event_index_output = highest_finished_event_index
                        .map(|index| format!(
                            "\nHighest finished event index: {index}"
                        )).unwrap_or_default(),
                    checkpoint_heading = "Checkpoint Downloading Progress".bold().walrus_teal(),
                    checkpoint_info = match (
                        latest_seq,
                        health_info.latest_checkpoint_sequence_number,
                    ) {
                        (Some(latest_seq), Some(checkpoint_seq)) => {
                            if latest_seq > checkpoint_seq {
                                format!(
                                    "Latest checkpoint sequence number: {}\
                                    \nEstimated checkpoint lag: {}",
                                    checkpoint_seq, latest_seq - checkpoint_seq
                                )
                            } else {
                                format!("Latest checkpoint sequence number: {checkpoint_seq}\
                                \nCheckpoint: up-to-date"
                                )
                            }
                        },
                        _ => {
                            format!("Latest checkpoint sequence number: {}\
                            \nLatest sui checkpoint: {}",
                                health_info
                                    .latest_checkpoint_sequence_number
                                    .map_or("N/A".to_string(), |seq| format!("{seq}")),
                                latest_seq.map_or("N/A".to_string(), |seq| format!("{seq}"))
                            )
                        },
                    },
                    shard_heading = "Shard Summary".bold().walrus_teal(),
                    owned = health_info.shard_summary.owned,
                    read_only = health_info.shard_summary.read_only,
                    owned_status_heading = "Owned Shard Status".bold().walrus_teal(),
                    unknown = health_info.shard_summary.owned_shard_status.unknown,
                    ready = health_info.shard_summary.owned_shard_status.ready,
                    in_transfer = health_info.shard_summary.owned_shard_status.in_transfer,
                    in_recovery = health_info.shard_summary.owned_shard_status.in_recovery,
                };

                // Print shard details if available
                if let Some(detail) = &health_info.shard_detail {
                    if !detail.owned.is_empty() {
                        println!("\n{}", "Owned Shard Details".bold().walrus_teal());
                        for shard in &detail.owned {
                            println!("Shard {}: {:?}", shard.shard, shard.status);
                        }
                    }
                    if !detail.other.is_empty() {
                        println!("\n{}", "Other Shard Details".bold().walrus_teal());
                        for shard in &detail.other {
                            println!("Shard {}: {:?}", shard.shard, shard.status);
                        }
                    }
                }
            }
        }
    }
}

impl CliOutput for ServiceHealthInfoOutput {
    fn print_cli_output(&self) {
        println!("\n{}", "Walrus Service Health Information".bold());

        // Initialize summary counters
        let mut owned_shards = 0;
        let mut read_only_shards = 0;
        let mut node_statuses = std::collections::HashMap::new();
        let mut table = create_node_health_table();

        // Collect summary information while building the table
        for (idx, node) in self.health_info.iter().enumerate() {
            match &node.health_info {
                Err(_) => {
                    *node_statuses.entry("Error".to_string()).or_insert(0) += 1;
                }
                Ok(health_info) => {
                    owned_shards += health_info.shard_summary.owned;
                    read_only_shards += health_info.shard_summary.read_only;
                    *node_statuses
                        .entry(health_info.node_status.to_string())
                        .or_insert(0) += 1;
                }
            }
            node.print_cli_output(self.latest_seq);
            add_node_health_to_table(&mut table, node, idx);
        }
        if table.len() > 3 {
            println!("\n{}\n", "Summary".bold().walrus_purple());
            table.printstd();
            println!("\nTotal nodes: {}", self.health_info.len());
            println!("Owned shards: {owned_shards}");
            println!("Read-only shards: {read_only_shards}");

            println!("\n{}", "Node Status Breakdown".bold().walrus_purple());
            for (status, count) in &node_statuses {
                println!("{status}: {count}");
            }
        }
    }
}

/// Default style for tables printed to stdout.
fn default_table_format() -> format::TableFormat {
    format::FormatBuilder::new()
        .separators(
            &[
                format::LinePosition::Top,
                format::LinePosition::Bottom,
                format::LinePosition::Title,
            ],
            format::LineSeparator::new('-', '-', '-', '-'),
        )
        .padding(1, 1)
        .build()
}

fn blob_and_file_str(blob_id: &BlobId, file: &Option<PathBuf>) -> String {
    if let Some(file) = file {
        format!("{} (file: {})", blob_id, file.display())
    } else {
        format!("{blob_id}")
    }
}

/// Print the full information of the storage node to stdout.
fn print_single_storage_node_info(node: &StorageNodeInfo, node_idx: usize, n_shards: &NonZeroU16) {
    let n_owned = node.n_shards;
    let n_owned_percent = (n_owned as f64) / f64::from(n_shards.get()) * 100.0;
    printdoc!(
        "

        {heading}
        Owned shards: {n_owned} ({n_owned_percent:.2} %)
        Total stake: {stake}
        Node address: {network_address}
        Node ID: {node_id}
        Public key: {public_key}
        Network public key: {network_public_key}
        Owned shards:
        {shards}
        Write price vote: {write_price_vote}
        Storage price vote: {storage_price_vote}
        Node capacity vote: {node_capacity_vote}
        ",
        heading = format!("{}: {}", node_idx, node.name).bold().walrus_teal(),
        stake = HumanReadableFrost::from(node.stake),
        network_address = node.network_address,
        node_id = node.node_id,
        public_key = node.public_key,
        network_public_key = node.network_public_key,
        shards = DisplayShardList(&node.shard_ids),
        write_price_vote = HumanReadableFrost::from(node.voting_params.write_price),
        storage_price_vote = HumanReadableFrost::from(node.voting_params.storage_price),
        node_capacity_vote = HumanReadableBytes(node.voting_params.node_capacity),
    );
}

fn create_node_health_table() -> Table {
    let mut table = Table::new();
    table.set_format(default_table_format());
    table.set_titles(row![
        b->"Idx",
        b->"Name",
        b->"Node ID",
        b->"Address",
        bc->"# Shards\n(Ready / Owned)",
        b->"Status",
    ]);
    table
}

fn add_node_health_to_table(table: &mut Table, node: &NodeHealthOutput, node_idx: usize) {
    match &node.health_info {
        Ok(health_info) => {
            let shards_str = format!(
                "{} / {}",
                health_info.shard_summary.owned_shard_status.ready, health_info.shard_summary.owned
            );
            table.add_row(row![
                r->node_idx,
                node.node_name,
                node.node_id,
                node.node_url,
                c->shards_str,
                health_info.node_status,
            ]);
        }
        Err(error) => {
            // Truncate error message to 20 chars and add ellipsis if needed
            let mut error_msg = error.to_string();
            if error_msg.len() > 40 {
                error_msg = format!("{}...", &error_msg[..37]);
            }

            table.add_row(row![
                r->node_idx,
                node.node_name,
                node.node_id,
                node.node_url,
                c->"N/A",
                Fr->error_msg,
            ]);
        }
    }
}

impl CliOutput for GetBlobAttributeOutput {
    fn print_cli_output(&self) {
        if let Some(attribute) = &self.attribute {
            println!("\n{}", "Attribute".bold().walrus_purple());
            for (key, value) in attribute.iter() {
                println!("{key}: {value}");
            }
        } else {
            println!("No attribute found");
        }
    }
}

impl CliOutput for QuiltMetadata {
    fn print_cli_output(&self) {
        match self {
            QuiltMetadata::V1(metadata_v1) => metadata_v1.print_cli_output(),
        }
    }
}

impl CliOutput for QuiltMetadataV1 {
    fn print_cli_output(&self) {
        println!(
            "{}: {}",
            "Quilt Metadata V1".bold().walrus_purple(),
            self.quilt_id
        );

        construct_stored_quilt_patch_table(&client_types::get_stored_quilt_patches(
            &self.index.clone().into(),
            self.quilt_id,
        ))
        .printstd();
    }
}

impl CliOutput for ReadQuiltOutput {
    fn print_cli_output(&self) {
        if let Some(out) = &self.out {
            println!(
                "Retrieved {} blob{s} and saved to directory: {}",
                self.retrieved_blobs
                    .len()
                    .to_string()
                    .bold()
                    .walrus_purple(),
                out.display().to_string().bold().walrus_purple(),
                s = plural_ending(&self.retrieved_blobs),
            );
            for (i, blob) in self.retrieved_blobs.iter().enumerate() {
                println!("{}. Identifier: {}", i + 1, blob.identifier().bold());
                if blob.tags().is_empty() {
                    println!("   Tags: (none)");
                } else {
                    println!("   Tags:");
                    for (key, value) in blob.tags() {
                        println!("     {key}: {value}");
                    }
                }
                println!();
            }
        } else {
            // TODO(WAL-858): Find a better way to print the blobs to stdout.
            for blob in &self.retrieved_blobs {
                if let Err(e) = std::io::stdout().write_all(blob.data()) {
                    eprintln!("Error writing {} to stdout: {e}", blob.identifier());
                }
            }
        }
    }
}

/// Prints a list of items separated by commas.
fn print_list<T: ToString>(items: &[T]) -> String {
    items.iter().map(ToString::to_string).join(", ")
}

/// Returns "s" if the number of items is not 1, otherwise an empty string.
fn plural_ending<T>(items: &[T]) -> &'static str {
    if items.len() == 1 { "" } else { "s" }
}
