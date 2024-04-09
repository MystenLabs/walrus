// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Bindings to call the Walrus contracts from Rust.

#[macro_use]
mod utils;
pub mod client;
pub mod contracts;
pub mod test_utils;
pub mod types;

pub use utils::get_created_sui_object_ids_by_type;
