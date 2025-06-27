// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Event streaming library for Walrus.
//!
//! This crate provides gRPC-based event streaming functionality with external
//! proto dependencies managed by buf, including Sui RPC types.

// Allow clippy warnings for generated protobuf code
#![allow(clippy::doc_lazy_continuation)]
#![allow(clippy::doc_markdown)]
#![allow(clippy::doc_overindented_list_items)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::large_enum_variant)]

// Include the generated protobuf code
include!(concat!(env!("OUT_DIR"), "/mod.rs"));

pub use walrus::event::v1alpha::*;
