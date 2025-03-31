// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Logic to handle the communication between the client and the storage nodes.

mod factory;
mod node;

pub(crate) use factory::NodeCommunicationFactory;
pub(crate) use node::{
    NodeCommunication,
    NodeReadCommunication,
    NodeResult,
    NodeWriteCommunication,
};
