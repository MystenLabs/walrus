// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use serde::{Deserialize, Serialize};

use crate::SliverType;

/// The maximum length in bytes of a single symbol in RaptorQ.
pub const MAX_SYMBOL_SIZE: u16 = u16::MAX;

/// The maximum number of source symbols per block for RaptorQ.
///
/// See `K'_max` in
/// [RFC 6330, Section 5.1.2](<https://datatracker.ietf.org/doc/html/rfc6330#section-5.1.2>)
pub const MAX_SOURCE_SYMBOLS_PER_BLOCK: u16 = 56403;

/// Marker trait to indicate the encoding axis (primary or secondary).
pub trait EncodingAxis: Clone + PartialEq + Eq + Default + core::fmt::Debug {
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
}

/// Marker type to indicate the primary encoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Primary;
impl EncodingAxis for Primary {
    type OrthogonalAxis = Secondary;
    const IS_PRIMARY: bool = true;
    const NAME: &'static str = "primary";
}

/// Marker type to indicate the secondary encoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Secondary;
impl EncodingAxis for Secondary {
    type OrthogonalAxis = Primary;
    const IS_PRIMARY: bool = false;
    const NAME: &'static str = "secondary";
}
