// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Utility functions used internally in the crate.

pub(crate) fn string_prefix<T: ToString>(s: &T) -> String {
    let mut string = s.to_string();
    string.truncate(8);
    format!("{}...", string)
}
