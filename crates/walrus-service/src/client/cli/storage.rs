// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Implementation of the storage manager and its CLI interface.
//!
//! A collection of utilities to manage storage in the wallet through the CLI.

use std::time::Duration;

use anyhow::Result;
use chrono::Utc;
use sui_types::base_types::ObjectID;
use walrus_core::Epoch;
use walrus_sui::{
    client::{ExpirySelectionPolicy, FuseStorageOperation, SuiClientResult, SuiContractClient},
    types::{StorageResource, move_structs::EpochState},
};

use super::{
    CliOutput as _,
    HumanReadableBytes,
    args::{EpochArg, SplitBy, StorageCommands, UserConfirmation, WhichFuse},
    runner::{ask_for_confirmation, get_end_epoch_and_epochs_ahead, get_epochs_ahead},
};
use crate::client::responses::{
    BuyStorageOutput,
    DestroyStorageOutput,
    ListStorageOutput,
    SplitStorageOutput,
};

/// The CLI interface to the [`StorageManager`].
pub(crate) struct StorageManagerCli {
    manager: StorageManager,
    json: bool,
}

impl StorageManagerCli {
    pub fn new(sui_client: SuiContractClient, json: bool) -> Self {
        StorageManagerCli {
            manager: StorageManager::new(sui_client),
            json,
        }
    }

    fn sui_client(&self) -> &SuiContractClient {
        &self.manager.sui_client
    }

    /// Runs the specified storage command, printing the result to stdout.
    pub async fn run_storage_command(&self, command: StorageCommands) -> Result<()> {
        match command {
            StorageCommands::List {
                include_expired,
                sort_by_size,
            } => self.list_storage(include_expired, sort_by_size).await,
            StorageCommands::Buy { epoch_arg, size } => self.buy_storage(epoch_arg, size).await,
            StorageCommands::Destroy { object_id, yes } => {
                self.destroy_storage(object_id, yes.into()).await
            }
            StorageCommands::Split {
                object_id,
                split_by,
            } => self.split_storage(object_id, split_by).await,
            StorageCommands::Fuse {
                object_ids,
                which_fuse,
                strict,
            } => self.fuse_storage(&object_ids, which_fuse, strict).await,
        }
    }

    /// Lists the storage resources owned by the wallet, printing the results to stdout.
    pub async fn list_storage(&self, include_expired: bool, sort_by_size: bool) -> Result<()> {
        let staking_object = self.sui_client().read_client.get_staking_object().await?;
        let current_epoch = staking_object.epoch();
        let epoch_duration = Duration::from_millis(staking_object.epoch_duration());
        let epoch_state = staking_object.epoch_state();

        let current_epoch_start_time = match epoch_state {
            EpochState::EpochChangeDone(epoch_start)
            | EpochState::NextParamsSelected(epoch_start) => *epoch_start,
            EpochState::EpochChangeSync(_) => Utc::now(),
        };

        let storage_resources = self.manager.list_storage(include_expired).await?;
        let output = ListStorageOutput::new_sorted(
            storage_resources,
            current_epoch,
            current_epoch_start_time,
            epoch_duration,
            sort_by_size,
        );

        output.print_output(self.json)
    }

    /// Destroys a storage resource object, printing the result to stdout.
    pub async fn destroy_storage(
        &self,
        object_id: ObjectID,
        confirmation: UserConfirmation,
    ) -> Result<()> {
        if confirmation.is_required() && !self.json {
            println!(
                "You are about to destroy storage resource {object_id}. \
                This action cannot be undone and provides no refund.",
            );
            if !ask_for_confirmation()? {
                println!("Storage destruction cancelled.");
                return Ok(());
            }
        }
        self.manager.destroy_storage(object_id).await?;
        let output = DestroyStorageOutput { object_id };
        output.print_output(self.json)
    }

    /// Splits a storage resource by size or epoch, printing the result to stdout.
    pub async fn split_storage(&self, object_id: ObjectID, split_by: SplitBy) -> Result<()> {
        let split_command = self.convert_split_by_to_command(split_by).await?;

        let (split_type, resulting_storage_objects) = match &split_command {
            SplitCommand::AtSize(split_size) => {
                let objects = self
                    .manager
                    .split_storage(object_id, split_command.clone())
                    .await?;
                (format!("size {}", HumanReadableBytes(*split_size)), objects)
            }
            SplitCommand::AtEpoch(target_epoch) => {
                let objects = self
                    .manager
                    .split_storage(object_id, split_command.clone())
                    .await?;
                (format!("epoch {target_epoch}"), objects)
            }
        };

        let output = SplitStorageOutput {
            original_object_id: object_id,
            resulting_storage_objects,
            split_type,
        };

        output.print_output(self.json)
    }

    /// Buys storage for the specified size, printing the result to stdout.
    pub async fn buy_storage(&self, epoch_arg: EpochArg, size: u64) -> Result<()> {
        anyhow::ensure!(size > 0, "the specified size must be greater than 0");

        let system_object = self.sui_client().read_client.get_system_object().await?;
        let staking_object = self.sui_client().read_client.get_staking_object().await?;

        let epochs_ahead = get_epochs_ahead(
            epoch_arg,
            system_object.max_epochs_ahead(),
            &self.sui_client(),
        )
        .await?;

        let storage_resource = self.manager.buy_storage(epochs_ahead, size).await?;

        let output = BuyStorageOutput {
            storage_resource,
            size_bytes: size,
            current_epoch: staking_object.epoch(),
        };

        output.print_output(self.json)
    }

    /// Fuses the storage resources into a single storage resource, printing the result to stdout.
    pub async fn fuse_storage(
        &self,
        object_ids: &[ObjectID],
        which_fuse: WhichFuse,
        strict: bool,
    ) -> Result<()> {
        let operation: FuseStorageOperation = which_fuse.try_into()?;

        self.manager.fuse_storage(object_ids, operation, strict);
        todo!()
    }

    /// Converts [`SplitBy`] to [`SplitCommand`], resolving epoch arguments if necessary.
    async fn convert_split_by_to_command(&self, split_by: SplitBy) -> Result<SplitCommand> {
        if let Some(split_size) = split_by.size {
            Ok(SplitCommand::AtSize(split_size))
        } else if let Some(epoch_arg) = split_by.epoch_arg {
            let system_object = self.sui_client().read_client.get_system_object().await?;
            let (end_epoch, _) = get_end_epoch_and_epochs_ahead(
                epoch_arg,
                system_object.max_epochs_ahead(),
                self.sui_client(),
            )
            .await?;

            Ok(SplitCommand::AtEpoch(end_epoch))
        } else {
            anyhow::bail!(
                "Must specify either --size or epoch arguments \
                (--epochs, --end-epoch, or --earliest-expiry-time)"
            );
        }
    }
}

/// A collection of utilities to manage storage owned by the wallet.
// REVIEW: This struct could be moved to the SDK, such that it can be used by other clients as well?
pub struct StorageManager {
    /// The Sui client used by the storage manager.
    sui_client: SuiContractClient,
}

impl StorageManager {
    pub fn new(sui_client: SuiContractClient) -> Self {
        StorageManager { sui_client }
    }

    pub async fn list_storage(
        &self,
        include_expired: bool,
    ) -> SuiClientResult<Vec<StorageResource>> {
        self.sui_client
            .owned_storage(ExpirySelectionPolicy::from_include_expired_flag(
                include_expired,
            ))
            .await
    }

    /// Buys the specified storage resource.
    pub async fn buy_storage(
        &self,
        epochs_ahead: Epoch,
        size: u64,
    ) -> SuiClientResult<StorageResource> {
        self.sui_client.reserve_space(size, epochs_ahead).await
    }

    /// Destroys the specified storage resource.
    pub async fn destroy_storage(&self, storage_object_id: ObjectID) -> SuiClientResult<()> {
        self.sui_client.destroy_storage(storage_object_id).await
    }

    /// Splits the storage, returning the resulting storage resources.
    pub async fn split_storage(
        &self,
        storage_object_id: ObjectID,
        split_command: SplitCommand,
    ) -> SuiClientResult<Vec<StorageResource>> {
        match split_command {
            SplitCommand::AtSize(split_at) => {
                self.sui_client
                    .split_storage_by_size(storage_object_id, split_at)
                    .await
            }
            SplitCommand::AtEpoch(split_at) => {
                self.sui_client
                    .split_storage_by_epoch(storage_object_id, split_at)
                    .await
            }
        }
    }

    pub fn fuse_storage(
        &self,
        object_ids: &[ObjectID],
        fuse_operation: FuseStorageOperation,
        strict: bool,
    ) {
        todo!()
    }
}

#[derive(Clone)]
pub(crate) enum SplitCommand {
    /// The size at which to split the storage resource.
    AtSize(u64),
    /// The epoch at which to split the storage resource.
    AtEpoch(Epoch),
}
