// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::io::stdout;

use anyhow::Result;
use colored::Colorize;
use indoc::printdoc;
use prettytable::{format, row, Table};
use serde::Serialize;
use walrus_sdk::api::BlobStatus;
use walrus_sui::types::Blob;

use crate::client::{
    cli_utils::{
        error,
        format_event_id,
        success,
        thousands_separator,
        HumanReadableBytes,
        HumanReadableMist,
    },
    responses::{
        BlobIdConversionOutput,
        BlobIdOutput,
        BlobStatusOutput,
        DryRunOutput,
        ExampleBlobInfo,
        InfoDevOutput,
        InfoOutput,
        ReadOutput,
    },
    string_prefix,
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

impl CliOutput for BlobStoreResult {
    fn print_cli_output(&self) {
        match &self {
            Self::AlreadyCertified {
                blob_id,
                event,
                end_epoch,
            } => {
                println!(
                    "{} Blob was previously certified within Walrus for a sufficient period.\n\
                    Blob ID: {}\nCertification event ID: {}\nEnd epoch (exclusive): {}",
                    success(),
                    blob_id,
                    format_event_id(event),
                    end_epoch,
                )
            }
            Self::NewlyCreated {
                blob_object,
                encoded_size,
                cost,
            } => {
                println!(
                    "{} Blob stored successfully.\n\
                    Blob ID: {}\n\
                    Unencoded size: {}\n\
                    Encoded size (including metadata): {}\n\
                    Sui object ID: {}\n\
                    Cost (excluding gas): {}",
                    success(),
                    blob_object.blob_id,
                    HumanReadableBytes(blob_object.size),
                    HumanReadableBytes(*encoded_size),
                    blob_object.id,
                    HumanReadableMist(*cost),
                )
            }
            Self::MarkedInvalid { blob_id, event } => {
                println!(
                    "{} Blob was marked as invalid.\nBlob ID: {}\nInvalidation event ID: {}",
                    error(),
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
                Blob ID: {}\n\
                Unencoded size: {}\n\
                Encoded size (including metadata): {}\n\
                Cost (excluding gas): {}\n",
            success(),
            self.blob_id,
            HumanReadableBytes(self.unencoded_size),
            HumanReadableBytes(self.encoded_size),
            HumanReadableMist(self.storage_cost),
        )
    }
}

impl CliOutput for BlobStatusOutput {
    fn print_cli_output(&self) {
        let blob_str = if let Some(file) = self.file.clone() {
            format!("{} (file: {})", self.blob_id, file.display())
        } else {
            format!("{}", self.blob_id)
        };
        match self.status {
            BlobStatus::Nonexistent => println!("Blob ID {blob_str} is not stored on Walrus."),
            BlobStatus::Existent {
                status,
                end_epoch,
                status_event,
            } => println!(
                "Status for blob ID {blob_str}: {}\n\
                    End epoch: {}\n\
                    Related event: {}",
                status.to_string().bold(),
                end_epoch,
                format_event_id(&status_event),
            ),
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
            price_per_unit_size,
            current_epoch,
            n_shards,
            n_nodes,
            max_blob_size,
            metadata_price,
            marginal_size,
            marginal_price,
            example_blobs,
            dev_info,
        } = self;

        // NOTE: keep text in sync with changes in the contracts.
        printdoc!(
            "

            {top_heading}
            Current epoch: {current_epoch}

            {storage_heading}
            Number of nodes: {n_nodes}
            Number of shards: {n_shards}

            {size_heading}
            Maximum blob size: {hr_max_blob} ({max_blob_size_sep} B)
            Storage unit: {hr_storage_unit}

            {price_heading}
            Price per encoded storage unit: {hr_price_per_unit_size}
            Price to store metadata: {metadata_price}
            Marginal price per additional {marginal_size:.0} (w/o metadata): {marginal_price}

            {price_examples_heading}
            {example_blob_output}
            ",
            top_heading = "Walrus system information".bold(),
            storage_heading = "Storage nodes".bold().green(),
            size_heading = "Blob size".bold().green(),
            hr_max_blob = HumanReadableBytes(*max_blob_size),
            hr_storage_unit = HumanReadableBytes(*unit_size),
            max_blob_size_sep = thousands_separator(*max_blob_size),
            price_heading = "Approximate storage prices per epoch".bold().green(),
            hr_price_per_unit_size = HumanReadableMist(*price_per_unit_size),
            metadata_price = HumanReadableMist(*metadata_price),
            marginal_size = HumanReadableBytes(*marginal_size),
            marginal_price = HumanReadableMist(*marginal_price),
            price_examples_heading = "Total price for example blob sizes".bold().green(),
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
            encoding_heading = "(dev) Encoding parameters and sizes".bold().yellow(),
            hr_metadata = HumanReadableBytes(*metadata_storage_size),
            metadata_storage_size_sep = thousands_separator(*metadata_storage_size),
            hr_sliver = HumanReadableBytes(*max_sliver_size),
            max_sliver_size_sep = thousands_separator(*max_sliver_size),
            hr_encoded = HumanReadableBytes(*max_encoded_blob_size),
            max_encoded_blob_size_sep = thousands_separator(*max_encoded_blob_size),
            bft_heading = "(dev) BFT system information".bold().yellow(),
            node_heading = "(dev) Storage node details and shard distribution"
                .bold()
                .yellow()
        );

        let mut table = Table::new();
        table.set_format(default_table_format());
        table.set_titles(row![b->"Idx", b->"# Shards", b->"Pk prefix", b->"Address"]);
        for (i, node) in storage_nodes.iter().enumerate() {
            let n_owned = node.n_shards;
            let n_owned_percent = (n_owned as f64) / (n_shards.get() as f64) * 100.0;
            table.add_row(row![
                bFg->format!("{i}"),
                format!("{} ({:.2}%)", n_owned, n_owned_percent),
                string_prefix(&node.public_key),
                node.network_address,
            ]);
        }
        table.printstd();
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
            bc->"Exp. epoch",
            b->"Object ID",
        ]);

        for blob in self {
            table.add_row(row![
                blob.blob_id,
                c->HumanReadableBytes(blob.size),
                c->blob.certified_epoch.is_some(),
                c->blob.storage.end_epoch,
                blob.id,
            ]);
        }
        table.printstd();
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
