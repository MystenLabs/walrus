// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/// Module: rotated
module walrus::pending;

use sui::vec_map::{Self, VecMap};

/// A value that contains values for E, E+1 and E+2.
public struct Pending(VecMap<u32, u64>) has store, copy, drop;

/// Create a new rotated value.
public fun empty(): Pending { Pending(vec_map::empty()) }

