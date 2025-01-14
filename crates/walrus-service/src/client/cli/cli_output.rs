// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{io::stdout, num::NonZeroU16, path::PathBuf};

use anyhow::Result;
use colored::Colorize;
use indoc::printdoc;
use prettytable::{format, row, Table};
use serde::Serialize;
use walrus_core::{BlobId, ShardIndex};
use walrus_sdk::api::{BlobStatus, DeletableCounts};
use walrus_sui::types::Blob;

use crate::client::{
    cli::{
        error,
        format_event_id,
        success,
        thousands_separator,
        HumanReadableBytes,
        HumanReadableFrost,
        HumanReadableMist,
        WalrusColors,
    },
    resource::RegisterBlobOp,
    responses::{
        BlobIdConversionOutput,
        BlobIdOutput,
        BlobStatusOutput,
        BlobStoreResultWithPath,
        DeleteOutput,
        DryRunOutput,
        ExampleBlobInfo,
        ExchangeOutput,
        ExtendBlobOutput,
        FundSharedBlobOutput,
        InfoDevOutput,
        InfoOutput,
        ReadOutput,
        ShareBlobOutput,
        StakeOutput,
        StorageNodeInfo,
        WalletOutput,
    },
    BlobStoreResult,
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
                    Blob ID: {}\n{event_or_object}\nExpiry epoch (exclusive): {}",
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
                };
                println!(
                    "{} {} blob stored successfully.\n\
                    Path: {}\n\
                    Blob ID: {}\n\
                    Sui object ID: {}\n\
                    Unencoded size: {}\n\
                    Encoded size (including replicated metadata): {}\n\
                    Cost (excluding gas): {} {}{}",
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
                    shared_blob_object
                        .map_or_else(String::new, |id| format!("\nShared blob object ID: {}", id))
                )
            }
            BlobStoreResult::MarkedInvalid { blob_id, event } => {
                println!(
                    "{} Blob was marked as invalid.\nPath: {}\nBlob ID: {}\n
                    Invalidation event ID: {}",
                    error(),
                    self.path.display(),
                    blob_id,
                    format_event_id(event),
                )
            }
        }
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
                Unencoded size: {}\nBlob ID: {}",
            success(),
            self.file.display(),
            self.unencoded_length,
            self.blob_id,
        )
    }
}

impl CliOutput for DryRunOutput {
    fn print_cli_output(&self) {
        println!(
            "{} Store dry-run succeeded.\n\
                Path: {}\n\
                Blob ID: {}\n\
                Unencoded size: {}\n\
                Encoded size (including replicated metadata): {}\n\
                Cost to store as new blob (excluding gas): {}\n",
            success(),
            self.path.display(),
            self.blob_id,
            HumanReadableBytes(self.unencoded_size),
            HumanReadableBytes(self.encoded_size),
            HumanReadableFrost::from(self.storage_cost),
        )
    }
}

impl CliOutput for BlobStatusOutput {
    fn print_cli_output(&self) {
        let blob_str = blob_and_file_str(&self.blob_id, &self.file);
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
                let initial_certified_str = if let Some(epoch) = initial_certified_epoch {
                    format!(", initially certified in epoch {}", epoch)
                } else {
                    "".to_string()
                };
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
                let initial_certified_str = if let Some(epoch) = initial_certified_epoch {
                    format!("\nInitially certified in epoch: {}", epoch,)
                } else {
                    "".to_string()
                };
                println!(
                    "There is a {status} permanent Blob object for blob ID {blob_str}.\n\
                        Expiry epoch: {end_epoch}\n\
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
            storage_unit_size: unit_size,
            storage_price_per_unit_size,
            write_price_per_unit_size,
            current_epoch,
            n_shards,
            n_nodes,
            max_blob_size,
            metadata_price,
            marginal_size,
            marginal_price,
            example_blobs,
            epoch_duration,
            max_epochs_ahead,
            dev_info,
        } = self;

        // NOTE: keep text in sync with changes in the contracts.
        printdoc!(
            "

            {top_heading}

            {epoch_heading}
            Current epoch: {current_epoch}
            Epoch duration: {hr_epoch_duration}
            Blobs can be stored for at most {max_epochs_ahead} epochs in the future.

            {storage_heading}
            Number of storage nodes: {n_nodes}
            Number of shards: {n_shards}

            {size_heading}
            Maximum blob size: {hr_max_blob} ({max_blob_size_sep} B)
            Storage unit: {hr_storage_unit}

            {price_heading}
            (Conversion rate: 1 WAL = 1,000,000,000 FROST)
            Price per encoded storage unit: {hr_storage_price_per_unit_size}
            Additional price for each write: {hr_write_price_per_unit_size}
            Price to store metadata: {metadata_price}
            Marginal price per additional {marginal_size:.0} (w/o metadata): {marginal_price}

            {price_examples_heading}
            {example_blob_output}
            ",
            top_heading = "Walrus system information".bold(),
            epoch_heading = "Epochs and storage duration".bold().walrus_teal(),
            hr_epoch_duration = humantime::format_duration(*epoch_duration),
            storage_heading = "Storage nodes".bold().walrus_teal(),
            size_heading = "Blob size".bold().walrus_teal(),
            hr_max_blob = HumanReadableBytes(*max_blob_size),
            hr_storage_unit = HumanReadableBytes(*unit_size),
            max_blob_size_sep = thousands_separator(*max_blob_size),
            price_heading = "Approximate storage prices per epoch".bold().walrus_teal(),
            hr_storage_price_per_unit_size = HumanReadableFrost::from(*storage_price_per_unit_size),
            hr_write_price_per_unit_size = HumanReadableFrost::from(*write_price_per_unit_size),
            metadata_price = HumanReadableFrost::from(*metadata_price),
            marginal_size = HumanReadableBytes(*marginal_size),
            marginal_price = HumanReadableFrost::from(*marginal_price),
            price_examples_heading = "Total price for example blob sizes".bold().walrus_teal(),
            example_blob_output = example_blobs
                .iter()
                .map(ExampleBlobInfo::cli_output)
                .collect::<Vec<_>>()
                .join("\n"),
        );

        let Some(InfoDevOutput {
            n_primary_source_symbols,
            n_secondary_source_symbols,
            metadata_storage_size,
            max_sliver_size,
            max_encoded_blob_size,
            max_faulty_shards,
            min_correct_shards,
            quorum_threshold,
            storage_nodes,
            next_storage_nodes,
            committee,
        }) = dev_info
        else {
            return;
        };

        let (min_nodes_above, shards_above) = committee.min_nodes_above_f();
        printdoc!(
            "

            {encoding_heading}
            Number of primary source symbols: {n_primary_source_symbols}
            Number of secondary source symbols: {n_secondary_source_symbols}
            Metadata size: {hr_metadata} ({metadata_storage_size_sep} B)
            Maximum sliver size: {hr_sliver} ({max_sliver_size_sep} B)
            Maximum encoded blob size: {hr_encoded} ({max_encoded_blob_size_sep} B)

            {bft_heading}
            Tolerated faults (f): {max_faulty_shards}
            Quorum threshold (2f+1): {quorum_threshold}
            Minimum number of correct shards (n-f): {min_correct_shards}
            Minimum number of nodes to get above f: {min_nodes_above} ({shards_above} shards)

            {node_heading}
            ",
            encoding_heading = "(dev) Encoding parameters and sizes".bold().walrus_purple(),
            hr_metadata = HumanReadableBytes(*metadata_storage_size),
            metadata_storage_size_sep = thousands_separator(*metadata_storage_size),
            hr_sliver = HumanReadableBytes(*max_sliver_size),
            max_sliver_size_sep = thousands_separator(*max_sliver_size),
            hr_encoded = HumanReadableBytes(*max_encoded_blob_size),
            max_encoded_blob_size_sep = thousands_separator(*max_encoded_blob_size),
            bft_heading = "(dev) BFT system information".bold().walrus_purple(),
            node_heading = "(dev) Storage node details and shard distribution"
                .bold()
                .walrus_purple()
        );

        print_storage_node_table(n_shards, storage_nodes);
        if let Some(storage_nodes) = next_storage_nodes.as_ref() {
            println!(
                "{}",
                "\n(dev) Next committee: Storage node details and shard distribution"
                    .bold()
                    .walrus_purple()
            );
            print_storage_node_table(n_shards, storage_nodes);
        };
    }
}

fn print_storage_node_table(n_shards: &NonZeroU16, storage_nodes: &[StorageNodeInfo]) {
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
        let n_owned_percent = (n_owned as f64) / (n_shards.get() as f64) * 100.0;
        table.add_row(row![
            bFc->format!("{i}"),
            node.name,
            r->format!("{} ({:.2}%)", n_owned, n_owned_percent),
            r->HumanReadableFrost::from(node.stake),
            node.network_address,
        ]);
    }
    table.printstd();
    for (i, node) in storage_nodes.iter().enumerate() {
        print_storage_node_info(node, i, n_shards);
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
        let blob_str = if let Some(blob_id) = self.blob_id {
            blob_and_file_str(&blob_id, &self.file)
        } else if let Some(object_id) = self.object_id {
            format!("(object ID: {})", object_id)
        } else {
            unreachable!("either file, blob ID or object ID must be provided")
        };

        if self.deleted_blobs.is_empty() {
            println!(
                "{} No objects were deleted for the given blob {blob_str}.",
                success()
            );
        } else {
            println!(
                "{} The following objects were deleted for the given blob {blob_str}:",
                success()
            );
            self.deleted_blobs.print_cli_output();
            if let Some(post_deletion_status) = self.post_deletion_status {
                let status_output = removed_instance_string(&post_deletion_status);

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
                    "".to_owned()
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
        println!("{} Staked WAL successfully.", success());
        println!("Staking info:\n{}", self.staked_wal);
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
            if let Some(amount) = self.amount {
                format!(", funded with {}", HumanReadableFrost::from(amount))
            } else {
                "".to_string()
            }
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
            self.epochs_ahead
        );
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
        format!("{}", blob_id)
    }
}

/// Print the full information of the storage node to stdoud.
fn print_storage_node_info(node: &StorageNodeInfo, node_idx: usize, n_shards: &NonZeroU16) {
    let n_owned = node.n_shards;
    let n_owned_percent = (n_owned as f64) / (n_shards.get() as f64) * 100.0;
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
        ",
        heading = format!("{}: {}", node_idx, node.name)
            .bold()
            .walrus_purple(),
        stake = HumanReadableFrost::from(node.stake),
        network_address = node.network_address,
        node_id = node.node_id,
        public_key = node.public_key,
        network_public_key = node.network_public_key,
        shards = DisplayShardList(&node.shard_ids),
    );
}
