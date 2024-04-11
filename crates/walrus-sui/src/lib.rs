// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Bindings to call the Walrus contracts from Rust.

#[macro_use]
mod utils;
pub mod client;
pub mod contracts;

#[cfg(any(test, feature = "test-utils"))]
pub mod test_utils;

pub mod types;
