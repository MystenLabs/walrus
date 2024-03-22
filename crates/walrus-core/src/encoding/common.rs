// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use super::get_encoding_config;
use crate::metadata::SliverPairIndex;

/// The maximum length in bytes of a single symbol in RaptorQ.
pub const MAX_SYMBOL_SIZE: usize = u16::MAX as usize;

/// The maximum number of source symbols per block for RaptorQ.
pub const MAX_SOURCE_SYMBOLS_PER_BLOCK: u16 = 56403;

/// The maximum number of shards that can be used, which is equivalent to the number of possible
/// encoding symbol IDs (ESI) in RaptorQ (each ESI is 24 bits).
pub const MAX_N_SHARDS: u32 = 1 << 24;

/// Marker trait to indicate the encoding axis (primary or secondary).
pub trait EncodingAxis: Clone + PartialEq + Eq + Default {
    /// The complementary encoding axis.
    type OrthogonalAxis: EncodingAxis;
    /// Whether this corresponds to the primary (true) or secondary (false) encoding.
    const IS_PRIMARY: bool;

    /// Computes the index of the [`Sliver`][super::Sliver] of the corresponding axis starting from
    /// the index of the [`SliverPair`][super::SliverPair].
    ///
    /// See [`super::EncodingConfig::sliver_index_from_pair_index`] for further details.
    fn sliver_index_from_pair_index(pair_index: SliverPairIndex) -> SliverPairIndex {
        get_encoding_config().sliver_index_from_pair_index::<Self>(pair_index)
    }
}

/// Marker type to indicate the primary encoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Primary;
impl EncodingAxis for Primary {
    type OrthogonalAxis = Secondary;
    const IS_PRIMARY: bool = true;

    fn sliver_index_from_pair_index(pair_index: SliverPairIndex) -> SliverPairIndex {
        pair_index
    }
}

/// Marker type to indicate the secondary encoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Secondary;
impl EncodingAxis for Secondary {
    type OrthogonalAxis = Primary;
    const IS_PRIMARY: bool = false;
}
