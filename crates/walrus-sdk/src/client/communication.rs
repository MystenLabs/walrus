// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Logic to handle the communication between the client and the storage nodes.

pub mod factory;
#[doc = "Node communication primitives."]
pub mod node;

pub use factory::NodeCommunicationFactory;
pub(crate) use node::{
    NodeCommunication,
    NodeReadCommunication,
    NodeResult,
    NodeWriteCommunication,
};
