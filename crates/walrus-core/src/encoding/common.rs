// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use serde::{Deserialize, Serialize};

use crate::SliverType;

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
