// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! The configuration for the fan-out proxy's tipping system.

use serde::{Deserialize, Serialize};
use sui_types::base_types::SuiAddress;

/// The kinds of tip that the proxy can choose to configure.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum TipKind {
    /// A constant tip.
    Const(u64),
    /// A tip that linearly depends on the encoded size of the blob.
    // TODO: make this linear in the KB or MB size of the blob?
    Linear { base: u64, encoded_size_mul: u64 },
    /// No tip is required.
    NoTip,
}

impl TipKind {
    /// Returns the tip required for a blob of the given size, or `None` if no tip is required.
    pub(crate) fn compute_tip(&self, encoded_size: u64) -> Option<u64> {
        match self {
            TipKind::Const(constant) => Some(*constant),
            TipKind::Linear {
                base,
                encoded_size_mul,
            } => Some(base + encoded_size * encoded_size_mul),
            TipKind::NoTip => None,
        }
    }
}

/// The configuration for the tips of to the proxy.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct TipConfig {
    /// The address to which the tip has to be sent to.
    pub address: SuiAddress,
    /// The kind of tip the proxy accepts.
    pub tip: TipKind,
}

impl TipConfig {
    /// Creates a new tip configuration.
    pub(crate) fn new(address: SuiAddress, tip: TipKind) -> Self {
        Self { address, tip }
    }
}
