// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Utilities used throughout Walrus.

use alloc::{
    format,
    string::{String, ToString},
    vec::Vec,
};

/// Creates a new struct transparently wrapping an unsigned integer.
///
/// Derives some default traits and implements standard conversions.
#[macro_export]
macro_rules! wrapped_uint {
    (
        $(#[$outer:meta])*
        $vis:vis struct $name:ident($visinner:vis $uint:ty) $({
            $( $inner:tt )*
        })?
    ) => {
        $(#[$outer])*
        #[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize, Hash)]
        #[repr(transparent)]
        #[serde(transparent)]
        $vis struct $name($visinner $uint);

        $(impl $name {
            /// Creates a new object from the given value.
            pub fn new(value: $uint) -> Self {
                Self(value)
            }

            /// Returns the wrapped value.
            pub fn get(self) -> $uint {
                self.0
            }

            $( $inner )*
        })?

        impl From<$name> for $uint {
            fn from(value: $name) -> Self {
                value.0
            }
        }

        impl From<$uint> for $name {
            fn from(value: $uint) -> Self {
                Self(value)
            }
        }

        impl From<&$uint> for $name {
            fn from(value: &$uint) -> Self {
                Self(*value)
            }
        }

        impl TryFrom<usize> for $name {
            type Error = core::num::TryFromIntError;

            fn try_from(value: usize) -> Result<Self, Self::Error> {
                Ok($name(value.try_into()?))
            }
        }

        impl TryFrom<u32> for $name {
            type Error = core::num::TryFromIntError;

            fn try_from(value: u32) -> Result<Self, Self::Error> {
                Ok($name(value.try_into()?))
            }
        }
    };
}

/// Returns a string that either contains the full `data` slice or its first few values separated by
/// commas.
///
/// # Examples
///
/// ```
/// # use walrus_core::utils::data_prefix_string;
/// #
/// assert_eq!(data_prefix_string(&Vec::<u8>::new(), 0), "[]");
/// assert_eq!(data_prefix_string(&Vec::<u8>::new(), 1), "[]");
/// assert_eq!(data_prefix_string(&Vec::<String>::new(), 0), "[]");
/// assert_eq!(data_prefix_string(&[1], 1), "[1]");
/// assert_eq!(data_prefix_string(&[1], 2), "[1]");
/// assert_eq!(data_prefix_string(&[1, 2, 3, 4], 4), "[1, 2, 3, 4]");
/// assert_eq!(data_prefix_string(&[1, 2, 3, 4], 10), "[1, 2, 3, 4]");
/// assert_eq!(data_prefix_string(&["1", "2", "3", "x"], 10), "[1, 2, 3, x]");
/// assert_eq!(data_prefix_string(&[1, 2, 3, 4], 1), "[1, ...]");
/// assert_eq!(data_prefix_string(&[1, 2, 3, 4], 3), "[1, 2, 3, ...]");
/// assert_eq!(data_prefix_string(&["x", "y", "z"], 1), "[x, ...]");
/// ```
#[inline]
pub fn data_prefix_string<T: ToString>(data: &[T], max_values_printed: usize) -> String {
    let data_items = data[..data.len().min(max_values_printed)]
        .iter()
        .map(T::to_string)
        .collect::<Vec<_>>()
        .join(", ");
    if data.len() <= max_values_printed {
        format!("[{data_items}]")
    } else {
        format!("[{data_items}, ...]")
    }
}
