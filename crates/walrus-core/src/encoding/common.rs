// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use core::num::NonZeroU16;

use serde::{Deserialize, Serialize};

use crate::SliverType;

/// Marker trait to indicate the encoding axis (primary or secondary).
pub trait EncodingAxis:
    Clone + PartialEq + Eq + Default + core::fmt::Debug + Send + Sync + 'static
{
    /// The complementary encoding axis.
    type OrthogonalAxis: EncodingAxis;
    /// Whether this corresponds to the primary (true) or secondary (false) encoding.
    const IS_PRIMARY: bool;
    /// String representation of this type.
    const NAME: &'static str;

    /// The associated [`SliverType`].
    fn sliver_type() -> SliverType {
        SliverType::for_encoding::<Self>()
    }

    /// Creates a `SliverPair` from a `SliverData<Self>`, filling in a dummy sliver for the
    /// orthogonal axis.
    fn make_sliver_pair_with_dummy(
        sliver: super::slivers::SliverData<Self>,
        symbol_size: NonZeroU16,
    ) -> super::slivers::SliverPair;
}

/// Marker type to indicate the primary encoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Primary;
impl EncodingAxis for Primary {
    type OrthogonalAxis = Secondary;
    const IS_PRIMARY: bool = true;
    const NAME: &'static str = "primary";

    fn make_sliver_pair_with_dummy(
        sliver: super::slivers::SliverData<Self>,
        symbol_size: NonZeroU16,
    ) -> super::slivers::SliverPair {
        use super::slivers::{SliverData, SliverPair};
        use crate::SliverIndex;

        // Create a dummy secondary sliver (it won't be used by decode_chunk)
        let dummy_secondary = SliverData::<Secondary>::new_empty(
            0, // length
            symbol_size,
            SliverIndex(0),
        );

        SliverPair {
            primary: sliver,
            secondary: dummy_secondary,
        }
    }
}

/// Marker type to indicate the secondary encoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Secondary;
impl EncodingAxis for Secondary {
    type OrthogonalAxis = Primary;
    const IS_PRIMARY: bool = false;
    const NAME: &'static str = "secondary";

    fn make_sliver_pair_with_dummy(
        sliver: super::slivers::SliverData<Self>,
        symbol_size: NonZeroU16,
    ) -> super::slivers::SliverPair {
        use super::slivers::{SliverData, SliverPair};
        use crate::SliverIndex;

        // Create a dummy primary sliver (it won't be used by decode_chunk)
        let dummy_primary = SliverData::<Primary>::new_empty(
            0, // length
            symbol_size,
            SliverIndex(0),
        );

        SliverPair {
            primary: dummy_primary,
            secondary: sliver,
        }
    }
}
