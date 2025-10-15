// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use core::fmt;

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

/// Types of consistency checks that can be performed after reconstructing a blob.
#[derive(Debug, Clone, Deserialize, Serialize, Default, Copy)]
pub enum ConsistencyCheckType {
    /// Skip consistency checks entirely.
    Skip,
    /// Default consistency check. This can differ for different encoding types. For the RS2
    /// encoding, this means checking the primary sliver hashes against the metadata.
    #[default]
    Default,
    /// Use the strict consistency check. This fully re-encodes the blob and checks the blob ID.
    Strict,
}

impl fmt::Display for ConsistencyCheckType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConsistencyCheckType::Skip => write!(f, "skip"),
            ConsistencyCheckType::Default => write!(f, "default"),
            ConsistencyCheckType::Strict => write!(f, "strict"),
        }
    }
}
