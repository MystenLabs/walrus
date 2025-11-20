// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Types for the checkpoint downloader.

use anyhow::Result;
use sui_types::{
    full_checkpoint_content::CheckpointData, messages_checkpoint::CheckpointSequenceNumber,
};
use tokio::sync::mpsc;

/// Worker message types.
pub(crate) enum WorkerMessage {
    /// Download a checkpoint with the given sequence number.
    Download(CheckpointSequenceNumber),
    /// Shutdown the worker.
    Shutdown,
}

/// Entry in the checkpoint fetcher queue.
#[derive(Debug)]
pub struct CheckpointEntry {
    /// The sequence number of the checkpoint.
    pub sequence_number: u64,
    /// The result of the checkpoint download.
    pub result: Result<CheckpointData>,
}

impl CheckpointEntry {
    /// Creates a new checkpoint entry.
    pub fn new(sequence_number: u64, result: Result<CheckpointData>) -> Self {
        Self {
            sequence_number,
            result,
        }
    }
}

/// Channels for the pool monitor.
#[derive(Clone)]
pub(crate) struct PoolMonitorChannels {
    pub(crate) message_sender: async_channel::Sender<WorkerMessage>,
    pub(crate) message_receiver: async_channel::Receiver<WorkerMessage>,
    pub(crate) checkpoint_sender: mpsc::Sender<CheckpointEntry>,
}
