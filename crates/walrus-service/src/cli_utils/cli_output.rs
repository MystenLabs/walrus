// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use anyhow::Result;
use colored::Colorize;
use serde::Serialize;
use walrus_sdk::api::BlobStatus;

use crate::{
    cli_utils::{error, format_event_id, success, HumanReadableBytes, HumanReadableMist},
    client::{BlobIdOutput, BlobStatusOutput, BlobStoreResult, DryRunOutput, ReadOutput},
};

/// Trait to differentiate output depending on the output mode.
pub trait CliOutput: Serialize {
    /// Returns the output as a human-readable string for the CLI.
    fn cli_output(&self) -> String;

    /// Returns the output as a string formatted depending on the output mode.
    fn output_string(&self, json: bool) -> Result<String> {
        if json {
            Ok(serde_json::to_string(&self)?)
        } else {
            Ok(self.cli_output())
        }
    }

    /// Writes the output to stdout formatted depending on the output mode.
    fn print_output(&self, json: bool) -> Result<()> {
        println!("{}", self.output_string(json)?);
        Ok(())
    }
}

impl CliOutput for BlobStoreResult {
    fn cli_output(&self) -> String {
        match &self {
            Self::AlreadyCertified {
                blob_id,
                event,
                end_epoch,
            } => {
                format!(
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
                format!(
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
                format!(
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
    fn cli_output(&self) -> String {
        match &self.out {
            Some(path) => {
                format!(
                    "{} Blob {} reconstructed from Walrus and written to {}.",
                    success(),
                    self.blob_id,
                    path.display()
                )
            }
            // The full blob has been written to stdout.
            None => "".into(),
        }
    }
}

impl CliOutput for BlobIdOutput {
    fn cli_output(&self) -> String {
        format!(
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
    fn cli_output(&self) -> String {
        format!(
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
    fn cli_output(&self) -> String {
        let blob_str = if let Some(file) = self.file.clone() {
            format!("{} (file: {})", self.blob_id, file.display())
        } else {
            format!("{}", self.blob_id)
        };
        match self.status {
            BlobStatus::Nonexistent => format!("Blob ID {blob_str} is not stored on Walrus."),
            BlobStatus::Existent {
                status,
                end_epoch,
                status_event,
            } => format!(
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
