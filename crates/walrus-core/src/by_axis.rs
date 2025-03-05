// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Structs related to the encoding axis.
//!
//! This module contains the [`ByAxis<T, U>`] type for creating types parameterised by a value for
//! the primary and secondary axes.
//!
//! Additionally provides the [`by_axis::flat_map`][`flat_map`] macro that can be used to apply
//! the same operation to the value stored in the primary or secondary variants of [`ByAxis`].
use alloc::{string::String, vec::Vec};
use core::fmt::{self, Display};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::encoding::EncodingAxis;

/// A type indicating either the primary or secondary axis.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize, Hash)]
#[serde(rename_all = "lowercase")]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub enum Axis {
    /// Enum indicating the primary axis.
    Primary,
    /// Enum indicating the secondary axis
    Secondary,
}

impl Axis {
    /// Returns the orthogonal axis.
    pub fn orthogonal(&self) -> Self {
        match self {
            Self::Primary => Self::Secondary,
            Self::Secondary => Self::Primary,
        }
    }

    /// Creates the [`Axis`] for the [`EncodingAxis`].
    pub fn for_encoding<T: EncodingAxis>() -> Self {
        if T::IS_PRIMARY {
            Self::Primary
        } else {
            Self::Secondary
        }
    }

    /// Provides a string representation of the enum variant.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Primary => "primary",
            Self::Secondary => "secondary",
        }
    }
}

impl AsRef<str> for Axis {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl Display for Axis {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Represents either a value over the primary ([`Primary`][Self::Primary]) or secondary encoding
/// axis ([`Secondary`][Self::Secondary]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ByAxis<T, U> {
    /// Contains the primary axis value.
    Primary(T),
    /// Contains the secondary axis value.
    Secondary(U),
}

impl<T, U> ByAxis<T, U> {
    /// Returns true iff this is the primary variant.
    pub fn is_primary(&self) -> bool {
        matches!(self, Self::Primary(_))
    }

    /// Returns true iff this is the secondary variant.
    pub fn is_secondary(&self) -> bool {
        matches!(self, Self::Secondary(_))
    }

    /// Returns the associated Axis.
    pub fn r#type(&self) -> Axis {
        match self {
            Self::Primary(_) => Axis::Primary,
            Self::Secondary(_) => Axis::Secondary,
        }
    }

    /// Converts from `&ByAxis<T, U>` to `ByAxis<&T, &U>`.
    ///
    /// Produces a new `ByAxis`, containing a reference into the original,
    /// leaving the original in place.
    pub fn as_ref(&self) -> ByAxis<&T, &U> {
        match self {
            ByAxis::Primary(inner) => ByAxis::Primary(inner),
            ByAxis::Secondary(inner) => ByAxis::Secondary(inner),
        }
    }

    /// Maps a `ByAxis<T, U>` to a value `O` by applying functions resulting in the same type to
    /// either variant.
    ///
    /// See also [`by_axis::flat_map!`][flat_map] when both `map_primary` and `map_secondary` are
    /// identical.
    pub fn flat_map<O, F, G>(self, map_primary: F, map_secondary: G) -> O
    where
        F: FnOnce(T) -> O,
        G: FnOnce(U) -> O,
    {
        match self {
            ByAxis::Primary(inner) => map_primary(inner),
            ByAxis::Secondary(inner) => map_secondary(inner),
        }
    }
}

/// Calls [`ByAxis::flat_map`] with the closure duplicated across both the `Primary`
/// and `Secondary` variants.
#[macro_export]
macro_rules! flat_map {
    ($by_axis:expr, $($fn_once:tt)*) => {
        $by_axis.flat_map($($fn_once)*, $($fn_once)*)
    };
}

pub(crate) use flat_map;

macro_rules! derive_from_trait {
    (ByAxis<$t:ty, $u:ty>, ($($type_args:tt)*)) => {
        impl<$($type_args)*> From<$t> for ByAxis<$t, $u> {
            fn from(value: $t) -> Self {
                Self::Primary(value)
            }
        }

        impl<$($type_args)*> From<$u> for ByAxis<$t, $u> {
            fn from(value: $u) -> Self {
                Self::Secondary(value)
            }
        }
    };

    (ByAxis<$t:ty, $u:ty>) => {
        $crate::by_axis::derive_from_trait!(ByAxis<$t, $u>, ());
    };
}

/// Error returned when trying to extract the wrong variant (primary or secondary).
#[derive(Debug, Error, PartialEq, Eq, Clone)]
#[error("wrong axis variant")]
pub struct WrongAxisError;

macro_rules! derive_try_from_trait {
    (ByAxis<$t:ty, $u:ty>, ($($type_args:tt)*)) => {
        impl<$($type_args)*> TryFrom<ByAxis<$t, $u>> for $t {
            type Error = WrongAxisError;

            fn try_from(value: ByAxis<$t, $u>) -> Result<Self, Self::Error> {
                match value {
                    ByAxis::Primary(value) => Ok(value),
                    ByAxis::Secondary(_) => Err(WrongAxisError),
                }
            }
        }

        impl<$($type_args)*> TryFrom<ByAxis<$t, $u>> for $u {
            type Error = WrongAxisError;

            fn try_from(value: ByAxis<$t, $u>) -> Result<Self, Self::Error> {
                match value {
                    ByAxis::Primary(_) => Err(WrongAxisError),
                    ByAxis::Secondary(value) => Ok(value),
                }
            }
        }
    };

    (ByAxis<$t:ty, $u:ty>) => {
        $crate::by_axis::derive_try_from_trait!(ByAxis<$t, $u>, ());
    };
}

pub(crate) use derive_from_trait;
pub(crate) use derive_try_from_trait;
