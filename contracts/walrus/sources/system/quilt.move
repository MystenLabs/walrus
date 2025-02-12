// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/// Contains the metadata for Blobs on Walrus.
module walrus::quilt;

use std::string::String;
use sui::vec_map::{Self, VecMap};


/// The metadata struct for Blob objects.
public struct QuiltingTaskManager has store {
    id: UID,
    tasks: VecMap<u256, QuiltingTask>,
}

/// Represents the state of a quilting task
const QUILT_STATE_INIT: u8 = 0;
const QUILT_STATE_BLOB_SELECTED: u8 = 1;
const QUILT_STATE_BLOB_CREATED: u8 = 2;
const QUILT_STATE_SUCCESS: u8 = 3;
const QUILT_STATE_ERROR: u8 = 4;

/// Represents a quilting task with its metadata and state
public struct QuiltingTask has store, drop {
    /// ID of the quilter shard that is the leader for this task
    leader_id: ID,
    /// Unique identifier for the task
    task_id: u256,
    /// Current state of the quilting process
    /// 0: Init
    /// 1: BlobSelected
    /// 2: BlobCreated
    /// 3: Success
    /// 4: Error
    state: u8,
    /// The blobs selected for this task
    blobs: vector<u256>,
}

/// Creates a new instance of QuiltingTaskManager.
public(package) fun new(ctx: &mut TxContext): QuiltingTaskManager {
    QuiltingTaskManager {
        id: object::new(ctx),
        tasks: vec_map::empty(),
    }
}

/// Add a new quilting task. Returns false if at capacity.
public(package) fun add_task(self: &mut QuiltingTaskManager, leader_id: ID, task_id: u256): bool {
    if (vec_map::size(&self.tasks) >= 1) {
        return false
    };
    if (vec_map::contains(&self.tasks, &task_id)) {
        return false
    };
    let task = QuiltingTask {
        leader_id,
        task_id,
        state: QUILT_STATE_INIT,
        blobs: vector::empty()
    };
    vec_map::insert(&mut self.tasks, task_id, task);
    true
}

/// Remove a quilting task. Returns false if task doesn't exist.
public(package) fun remove_task(self: &mut QuiltingTaskManager, task_id: u256): bool {
    if (!vec_map::contains(&self.tasks, &task_id)) {
        return false
    };
    vec_map::remove(&mut self.tasks, &task_id);
    true
}

/// Update state of an existing task. Returns false if task doesn't exist.
public(package) fun update_task_state(self: &mut QuiltingTaskManager, leader_id: ID, task_id: u256, new_state: u8): bool {
    if (!vec_map::contains(&self.tasks, &task_id)) {
        return false
    };
    let task = vec_map::get_mut(&mut self.tasks, &task_id);
    if (task.leader_id != leader_id) {
        return false
    };
    task.state = new_state;
    true
}