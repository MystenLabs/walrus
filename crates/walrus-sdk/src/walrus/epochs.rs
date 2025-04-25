// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Structures for use in the Walrus SDK related to epoch specification.
use std::{num::NonZeroU32, time::SystemTime};

use anyhow::{Result, anyhow};
use walrus_core::{Epoch, EpochCount, ensure};

/// The number of epochs to store the blob for.
///
/// Can be either a non-zero number of epochs or the special value `max`, which will store the blob
/// for the maximum number of epochs allowed by the system object on chain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EpochCountOrMax {
    /// Store the blob for the maximum number of epochs allowed.
    Max,
    /// The number of epochs to store the blob for.
    Epochs(NonZeroU32),
}

impl TryFrom<&str> for EpochCountOrMax {
    type Error = anyhow::Error;
    fn try_from(input: &str) -> Result<Self> {
        if input == "max" {
            Ok(Self::Max)
        } else {
            let epochs = input.parse::<u32>()?;
            Ok(Self::Epochs(NonZeroU32::new(epochs).ok_or_else(|| {
                anyhow!("invalid epoch count; please a number >0 or `max`")
            })?))
        }
    }
}

impl EpochCountOrMax {
    /// Tries to convert the `EpochCountOrMax` into an `EpochCount` value.
    ///
    /// If the `EpochCountOrMax` is `Max`, the `max_epochs_ahead` is used as the maximum number of
    /// epochs that can be stored ahead.
    pub fn try_into_epoch_count(&self, max_epochs_ahead: EpochCount) -> anyhow::Result<EpochCount> {
        match self {
            EpochCountOrMax::Max => Ok(max_epochs_ahead),
            EpochCountOrMax::Epochs(epochs) => {
                let epochs = epochs.get();
                ensure!(
                    epochs <= max_epochs_ahead,
                    "blobs can only be stored for up to {} epochs ahead; {} epochs were requested",
                    max_epochs_ahead,
                    epochs
                );
                Ok(epochs)
            }
        }
    }
}

/// The number of epochs to store the blob for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EpochArg {
    /// The number of epochs the blob is stored for.
    ///
    /// If set to `max`, the blob is stored for the maximum number of epochs allowed by the
    /// system object on chain. Otherwise, the blob is stored for the specified number of
    /// epochs. The number of epochs must be greater than 0.
    EpochCountOrMax(EpochCountOrMax),

    /// The earliest time when the blob can expire.
    EarliestExpiryTime(SystemTime),

    /// The end epoch for the blob.
    EndEpoch(Epoch),
}
