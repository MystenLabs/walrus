// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

// TODO:
// 1. registering the node
// 2. adding staked wal to the pool
// 3. selecting the committee
// 4. withdrawing staked wal from the pool
//
// NOTES:
// - advance_epoch - initiates the epoch change
// - initiate epoch change - bumped in `advance_epoch`
// - get "epoch_sync_done" event
module walrus::staking_inner;

use std::{fixed_point32::FixedPoint32, string::String};
use sui::{
    balance::{Self, Balance},
    clock::Clock,
    coin::Coin,
    object_table::{Self, ObjectTable},
    priority_queue::{Self, PriorityQueue},
    vec_map
};
use wal::wal::WAL;
use walrus::{
    active_set::{Self, ActiveSet},
    bls_aggregate::{Self, BlsCommittee},
    committee::{Self, Committee},
    epoch_parameters::{Self, EpochParams},
    events,
    staked_wal::StakedWal,
    staking_pool::{Self, StakingPool},
    storage_node::StorageNodeCap,
    walrus_context::{Self, WalrusContext}
};

/// The minimum amount of staked WAL required to be included in the active set.
const MIN_STAKE: u64 = 0;

/// Temporary upper limit for the number of storage nodes.
/// TODO: Remove once solutions are in place to prevent hitting move execution limits (#935).
const TEMP_ACTIVE_SET_SIZE_LIMIT: u16 = 100;

// TODO: decide epoch duration. Consider making it a system parameter.

// The duration of an epoch in ms.
// Currently one week.
// TODO: currently replaced by the epoch duration.
// const EPOCH_DURATION: u64 = 7 * 24 * 60 * 60 * 1000;

// The delta between the epoch change finishing and selecting the next epoch parameters in ms.
// Currently half of an epoch.
// TODO: currently replaced by the epoch duration / 2.
// const PARAM_SELECTION_DELTA: u64 = 7 * 24 * 60 * 60 * 1000 / 2;

// TODO: remove this once the module is implemented.
#[error]
const ENotImplemented: vector<u8> = b"Function is not implemented";

#[error]
const EWrongEpochState: vector<u8> = b"Current epoch state does not allow this operation";

#[error]
const EInvalidSyncEpoch: vector<u8> =
    b"The epoch of the epoch sync done call does not match the current epoch";

#[error]
const EDuplicateSyncDone: vector<u8> = b"Node already attested that sync is done for this epoch";

#[error]
const ENoStake: vector<u8> = b"Total stake is zero for apportionment";

#[error]
const ENotInCommittee: vector<u8> = b"The node is not part of the current committee";

/// The epoch state.
public enum EpochState has store, copy, drop {
    // Epoch change is currently in progress. Contains the weight of the nodes that
    // have already attested that they finished the sync.
    EpochChangeSync(u16),
    // Epoch change has been completed at the contained timestamp.
    EpochChangeDone(u64),
    // The parameters for the next epoch have been selected.
    // The contained timestamp is the start of the current epoch.
    NextParamsSelected(u64),
}

/// The inner object for the staking part of the system.
public struct StakingInnerV1 has store, key {
    /// The object ID
    id: UID,
    /// The number of shards in the system.
    n_shards: u16,
    /// The duration of an epoch in ms. Does not affect the first (zero) epoch.
    epoch_duration: u64,
    /// Special parameter, used only for the first epoch. The timestamp when the
    /// first epoch can be started.
    first_epoch_start: u64,
    /// Stored staking pools, each identified by a unique `ID` and contains
    /// the `StakingPool` object. Uses `ObjectTable` to make the pool discovery
    /// easier by avoiding wrapping.
    ///
    /// The key is the ID of the staking pool.
    pools: ObjectTable<ID, StakingPool>,
    /// The current epoch of the Walrus system. The epochs are not the same as
    /// the Sui epochs, not to be mistaken with `ctx.epoch()`.
    epoch: u32,
    /// Stores the active set of storage nodes. Provides automatic sorting and
    /// tracks the total amount of staked WAL.
    active_set: ActiveSet,
    /// The next committee in the system.
    next_committee: Option<Committee>,
    /// The current committee in the system.
    committee: Committee,
    /// The previous committee in the system.
    previous_committee: Committee,
    /// The next epoch parameters.
    next_epoch_params: Option<EpochParams>,
    /// The state of the current epoch.
    epoch_state: EpochState,
    /// Rewards left over from the previous epoch that couldn't be distributed due to rounding.
    leftover_rewards: Balance<WAL>,
}

/// Creates a new `StakingInnerV1` object with default values.
public(package) fun new(
    epoch_zero_duration: u64,
    epoch_duration: u64,
    n_shards: u16,
    clock: &Clock,
    ctx: &mut TxContext,
): StakingInnerV1 {
    StakingInnerV1 {
        id: object::new(ctx),
        n_shards,
        epoch_duration,
        first_epoch_start: epoch_zero_duration + clock.timestamp_ms(),
        pools: object_table::new(ctx),
        epoch: 0,
        active_set: active_set::new(TEMP_ACTIVE_SET_SIZE_LIMIT, MIN_STAKE),
        next_committee: option::none(),
        committee: committee::empty(),
        previous_committee: committee::empty(),
        next_epoch_params: option::none(),
        epoch_state: EpochState::EpochChangeDone(clock.timestamp_ms()),
        leftover_rewards: balance::zero(),
    }
}

// === Staking Pool / Storage Node ===

/// Creates a new staking pool with the given `commission_rate`.
public(package) fun create_pool(
    self: &mut StakingInnerV1,
    name: String,
    network_address: String,
    public_key: vector<u8>,
    network_public_key: vector<u8>,
    proof_of_possession: vector<u8>,
    commission_rate: u64,
    storage_price: u64,
    write_price: u64,
    node_capacity: u64,
    ctx: &mut TxContext,
): ID {
    let pool = staking_pool::new(
        name,
        network_address,
        public_key,
        network_public_key,
        proof_of_possession,
        commission_rate,
        storage_price,
        write_price,
        node_capacity,
        &self.new_walrus_context(),
        ctx,
    );

    let node_id = object::id(&pool);
    self.pools.add(node_id, pool);
    node_id
}

/// Blocks staking for the pool, marks it as "withdrawing".
#[allow(unused_mut_parameter)]
public(package) fun withdraw_node(self: &mut StakingInnerV1, cap: &mut StorageNodeCap) {
    let wctx = &self.new_walrus_context();
    self.pools[cap.node_id()].set_withdrawing(wctx);
}

public(package) fun collect_commission(_: &mut StakingInnerV1, _: &StorageNodeCap): Coin<WAL> {
    abort ENotImplemented
}

public(package) fun voting_end(self: &mut StakingInnerV1, clock: &Clock) {
    // Check if it's time to end the voting.
    let last_epoch_change = match (self.epoch_state) {
        EpochState::EpochChangeDone(last_epoch_change) => last_epoch_change,
        _ => abort EWrongEpochState,
    };

    let now = clock.timestamp_ms();
    let param_selection_delta = self.epoch_duration / 2;

    // We don't need a delay for the epoch zero.
    if (self.epoch != 0) {
        assert!(now >= last_epoch_change + param_selection_delta, EWrongEpochState);
    } else {
        assert!(now >= self.first_epoch_start, EWrongEpochState);
    };

    // Assign the next epoch committee.
    self.select_committee();
    self.next_epoch_params = option::some(self.calculate_votes());

    // Set the new epoch state.
    self.epoch_state = EpochState::NextParamsSelected(last_epoch_change);

    // Emit event that parameters have been selected.
    events::emit_epoch_parameters_selected(self.epoch + 1);
}

/// Calculates the votes for the next epoch parameters. The function sorts the
/// write and storage prices and picks the value that satisfies a quorum of the weight.
public(package) fun calculate_votes(self: &StakingInnerV1): EpochParams {
    assert!(self.next_committee.is_some());

    let size = self.next_committee.borrow().size();
    let inner = self.next_committee.borrow().inner();
    let mut write_prices = priority_queue::new(vector[]);
    let mut storage_prices = priority_queue::new(vector[]);
    let mut capacity_votes = priority_queue::new(vector[]);

    size.do!(|i| {
        let (node_id, shards) = inner.get_entry_by_idx(i);
        let pool = &self.pools[*node_id];
        let weight = shards.length();
        write_prices.insert(pool.write_price(), weight);
        storage_prices.insert(pool.storage_price(), weight);
        // The vote for capacity is determined by the node capacity and number of assigned shards.
        let capacity_vote = pool.node_capacity() / weight * (self.n_shards as u64);
        capacity_votes.insert(capacity_vote, weight);
    });

    epoch_parameters::new(
        quorum_above(&mut capacity_votes, self.n_shards),
        quorum_below(&mut storage_prices, self.n_shards),
        quorum_below(&mut write_prices, self.n_shards),
    )
}

/// Take the highest value, s.t. a quorum (2f + 1) voted for a value larger or equal to this.
fun quorum_above(vote_queue: &mut PriorityQueue<u64>, n_shards: u16): u64 {
    let threshold_weight = (n_shards - (n_shards - 1) / 3) as u64;
    take_threshold_value(vote_queue, threshold_weight)
}

/// Take the lowest value, s.t. a quorum  (2f + 1) voted for a value lower or equal to this.
fun quorum_below(vote_queue: &mut PriorityQueue<u64>, n_shards: u16): u64 {
    let threshold_weight = ((n_shards - 1) / 3 + 1) as u64;
    take_threshold_value(vote_queue, threshold_weight)
}

fun take_threshold_value(vote_queue: &mut PriorityQueue<u64>, threshold_weight: u64): u64 {
    let mut sum_weight = 0;
    // The loop will always succeed if `threshold_weight` is smaller than the total weight.
    loop {
        let (value, weight) = vote_queue.pop_max();
        sum_weight = sum_weight + weight;
        if (sum_weight >= threshold_weight) {
            return value
        };
    }
}

// === Voting ===

/// Sets the next commission rate for the pool.
public(package) fun set_next_commission(
    self: &mut StakingInnerV1,
    cap: &StorageNodeCap,
    commission_rate: u64,
) {
    self.pools[cap.node_id()].set_next_commission(commission_rate);
}

/// Sets the next storage price for the pool.
public(package) fun set_next_storage_price(
    self: &mut StakingInnerV1,
    cap: &StorageNodeCap,
    storage_price: u64,
) {
    self.pools[cap.node_id()].set_next_storage_price(storage_price);
}

/// Sets the next write price for the pool.
public(package) fun set_next_write_price(
    self: &mut StakingInnerV1,
    cap: &StorageNodeCap,
    write_price: u64,
) {
    self.pools[cap.node_id()].set_next_write_price(write_price);
}

/// Sets the next node capacity for the pool.
public(package) fun set_next_node_capacity(
    self: &mut StakingInnerV1,
    cap: &StorageNodeCap,
    node_capacity: u64,
) {
    self.pools[cap.node_id()].set_next_node_capacity(node_capacity);
}

// === Staking ===

/// Blocks staking for the pool, marks it as "withdrawing".
/// TODO: Is this action instant or should it be processed in the next epoch?
public(package) fun set_withdrawing(self: &mut StakingInnerV1, node_id: ID) {
    let wctx = &self.new_walrus_context();
    self.pools[node_id].set_withdrawing(wctx);
}

/// Destroys the pool if it is empty, after the last stake has been withdrawn.
public(package) fun destroy_empty_pool(
    self: &mut StakingInnerV1,
    node_id: ID,
    _ctx: &mut TxContext,
) {
    self.pools.remove(node_id).destroy_empty()
}

/// Stakes the given amount of `T` with the pool, returning the `StakedWal`.
public(package) fun stake_with_pool(
    self: &mut StakingInnerV1,
    to_stake: Coin<WAL>,
    node_id: ID,
    ctx: &mut TxContext,
): StakedWal {
    let wctx = &self.new_walrus_context();
    let pool = &mut self.pools[node_id];
    let staked_wal = pool.stake(to_stake.into_balance(), wctx, ctx);

    // active set only tracks the stake for the next epoch, pool already knows
    // whether the stake was applied to E+1 or E+2.
    self.active_set.insert_or_update(node_id, pool.wal_balance_at_epoch(wctx.epoch() + 1));

    staked_wal
}

/// Requests withdrawal of the given amount from the `StakedWAL`, marking it as
/// `Withdrawing`. Once the epoch is greater than the `withdraw_epoch`, the
/// withdrawal can be performed.
public(package) fun request_withdraw_stake(
    self: &mut StakingInnerV1,
    staked_wal: &mut StakedWal,
    _ctx: &mut TxContext,
) {
    let wctx = &self.new_walrus_context();
    self.pools[staked_wal.node_id()].request_withdraw_stake(staked_wal, wctx);
}

/// Perform the withdrawal of the staked WAL, returning the amount to the caller.
/// The `StakedWal` must be in the `Withdrawing` state, and the epoch must be
/// greater than the `withdraw_epoch`.
public(package) fun withdraw_stake(
    self: &mut StakingInnerV1,
    staked_wal: StakedWal,
    ctx: &mut TxContext,
): Coin<WAL> {
    let wctx = &self.new_walrus_context();
    self.pools[staked_wal.node_id()].withdraw_stake(staked_wal, wctx).into_coin(ctx)
}

// === System ===

/// Selects the committee for the next epoch.
///
/// TODO: current solution is temporary, we need to have a proper algorithm for shard assignment.
public(package) fun select_committee(self: &mut StakingInnerV1) {
    assert!(self.next_committee.is_none());

    let active_ids = self.active_set.active_ids();
    let values = self.apportionment();
    let distribution = vec_map::from_keys_values(active_ids, values);

    // if we're dealing with the first epoch, we need to assign the shards to the
    // nodes in a sequential manner. Assuming there's at least 1 node in the set.
    let committee = if (self.committee.size() == 0) committee::initialize(distribution)
    else self.committee.transition(distribution);

    self.next_committee = option::some(committee);
}

fun apportionment(self: &StakingInnerV1): vector<u16> {
    let active_ids = self.active_set.active_ids();
    let stake = active_ids.map_ref!(|node_id| self.active_set[node_id]);
    // TODO better ranking
    let ranking = {
        // TODO use std::vector::tabulate
        let mut v = vector[];
        stake.length().do!(|i| v.push_back(i));
        v
    };
    let (_price, shards) = dhondt(ranking, self.n_shards, stake);
    shards
}

// Implementation of the D'Hondt method (aka Jefferson method) for apportionment.
// TODO: currently because of Fixedpoint32, there is a "low" limit on the `total_stake`.
// Assuming 1000 shards (and ignoring the number of nodes), the limit for the total stake is
// 4,000,000,000,000 (4e12).
fun dhondt(
    // A ranking of the nodes by index in the `stake` vector. The lower the index, the higher the
    // rank. So the first index in this vector is the node that has the highest precedence for
    // additional shards.
    ranking: vector<u64>,
    n_shards: u16,
    stake: vector<u64>,
): (FixedPoint32, vector<u16>) {
    use std::fixed_point32::{
        divide_u64 as u64_div,
        create_from_rational as from_rational,
        create_from_raw_value as from_raw,
        get_raw_value as to_raw,
    };

    let total_stake = stake.fold!(0, |acc, x| acc + x);
    let n_nodes = stake.length();
    let n_shards = n_shards as u64;
    assert!(total_stake > 0, ENoStake);

    // initial price guess following Pukelsheim
    let mut price = from_rational(total_stake, n_shards + (n_nodes / 2));
    if (n_nodes == 0) return (price, vector[]);
    let mut shards = stake.map_ref!(|s| u64_div(*s, price));
    let mut n_shards_distributed = shards.fold!(0, |acc, x| acc + x);
    // loop until all shards are distributed
    while (n_shards_distributed != n_shards) {
        n_shards_distributed = if (n_shards_distributed < n_shards) {
                // We decrease the price slightly such that some nodes get an additional shard.
                price = from_raw(0);
                let mut at_threshold = vector[];
                let mut i = 0;
                stake.zip_do_ref!(&shards, |s, m| {
                    let threshold_price = from_rational(*s, *m + 1);
                    if (threshold_price.to_raw() > price.to_raw()) {
                        price = threshold_price;
                        at_threshold = vector[i];
                    } else if (threshold_price == price) {
                        at_threshold.push_back(i);
                    };
                    i = i + 1;
                });

                let adjusted_distribution = n_shards_distributed + at_threshold.length();
                if (adjusted_distribution <= n_shards) {
                    // We give one additional shard to all nodes with the same threshold price.
                    at_threshold.do!(|n| *&mut shards[n] = shards[n] + 1);
                    adjusted_distribution
                } else {
                    // If there are more nodes with the same threshold price than additional shards
                    // to distribute, we only give an additional shard to a subset according to
                    // their rank so that `n_shards_distributed == n_shards`.
                    let mut to_give = n_shards - n_shards_distributed;
                    // Iterate over the ranking, giving shards from the nodes with the highest
                    // rank first.
                    ranking.length().do!(|i| {
                        if (to_give == 0) return;
                        let idx = &ranking[i];
                        if (at_threshold.contains(idx)) {
                            *&mut shards[*idx] = shards[*idx] + 1;
                            to_give = to_give - 1;
                        }
                    });
                    n_shards
                }
            } else {
                // We increase the price slightly such that some nodes get one fewer shard.
                price = from_raw(0xFFFF_FFFF_FFFF_FFFF); // TODO: use std::u64::max_value!()
                let mut at_threshold = vector[];
                let mut i = 0;
                stake.zip_do_ref!(&shards, |s, m| {
                    let m = *m;
                    if (m != 0) {
                        let threshold_price = from_rational(*s, m);
                        if (threshold_price.to_raw() < price.to_raw()) {
                            price = threshold_price;
                            at_threshold = vector[i];
                        } else if (threshold_price == price) {
                            at_threshold.push_back(i);
                        }
                    };
                    i = i + 1;
                });
                // `at_threshold.length() <= n_shards_distributed` due to the check `m != 0` above.
                let adjusted_distribution = n_shards_distributed - at_threshold.length();
                if (adjusted_distribution >= n_shards) {
                    // We increase the price slightly above the threshold such that all nodes at
                    // the threshold lose one shard.
                    // price += 1 / 4000000000
                    price = price.add(from_raw(1));
                    at_threshold.do!(|n| *&mut shards[n] = shards[n] - 1);
                    adjusted_distribution
                } else {
                    // If there are more nodes with the same threshold price than shards to take
                    // away, we only remove a shard from a subset according to their rank so that
                    // `n_shards_distributed == n_shards`. In this case, the price remains at the
                    // threshold.
                    let mut to_take = n_shards_distributed - n_shards;
                    let n = ranking.length();
                    n.do!(|i| {
                        if (to_take == 0) return;
                        let idx = &ranking[n - 1 - i];
                        if (at_threshold.contains(idx)) {
                            *&mut shards[*idx] = shards[*idx] - 1;
                            to_take = to_take - 1;
                        }
                    });
                    n_shards
                }
            }
    };
    (price, shards.map!(|s| s as u16))
}

use fun fp_add as FixedPoint32.add;

fun fp_add(a: FixedPoint32, b: FixedPoint32): FixedPoint32 {
    use std::fixed_point32::{create_from_raw_value as from_raw, get_raw_value as to_raw};
    let sum = (a.to_raw() as u128) + (b.to_raw() as u128);
    // TODO use std::u64::max_value!()
    assert!(sum <= 0xFFFF_FFFF_FFFF_FFFF);
    from_raw(sum as u64)
}

/// Initiates the epoch change if the current time allows.
public(package) fun initiate_epoch_change(
    self: &mut StakingInnerV1,
    clock: &Clock,
    rewards: Balance<WAL>,
) {
    let last_epoch_change = match (self.epoch_state) {
        EpochState::NextParamsSelected(last_epoch_change) => last_epoch_change,
        _ => abort EWrongEpochState,
    };

    let now = clock.timestamp_ms();

    if (self.epoch == 0) assert!(now >= self.first_epoch_start, EWrongEpochState)
    else assert!(now >= last_epoch_change + self.epoch_duration, EWrongEpochState);

    self.advance_epoch(rewards);
}

/// Sets the next epoch of the system and emits the epoch change start event.
///
/// TODO: `advance_epoch` needs to be either pre or post handled by each staking pool as well.
public(package) fun advance_epoch(self: &mut StakingInnerV1, mut rewards: Balance<WAL>) {
    assert!(self.next_committee.is_some(), EWrongEpochState);

    self.epoch = self.epoch + 1;
    self.previous_committee = self.committee;
    self.committee = self.next_committee.extract(); // overwrites the current committee
    self.epoch_state = EpochState::EpochChangeSync(0);

    let wctx = &self.new_walrus_context();

    // Distribute the rewards.

    // Add any leftover rewards to the rewards to distribute.
    let leftover_value = self.leftover_rewards.value();
    rewards.join(self.leftover_rewards.split(leftover_value));
    let rewards_per_shard = rewards.value() / (self.n_shards as u64);

    // Add any nodes that are new in the committee to the previous shard assignments
    // without any shards, s.t. we call advance_epoch on them and update the active set.
    let mut prev_shard_assignments = *self.previous_committee.inner();
    self.committee.inner().keys().do!(|node_id| if (!prev_shard_assignments.contains(&node_id)) {
        prev_shard_assignments.insert(node_id, vector[]);
    });
    let (node_ids, shard_assignments) = prev_shard_assignments.into_keys_values();

    node_ids.zip_do!(shard_assignments, |node_id, shards| {
        self.pools[node_id].advance_epoch(rewards.split(rewards_per_shard * shards.length()), wctx);
        self.active_set.update(node_id, self.pools[node_id].wal_balance_at_epoch(wctx.epoch() + 1));
    });

    // Save any leftover rewards due to rounding.
    self.leftover_rewards.join(rewards);

    // Emit epoch change start event.
    events::emit_epoch_change_start(self.epoch);
}

/// Signals to the contract that the node has received all its shards for the new epoch.
public(package) fun epoch_sync_done(
    self: &mut StakingInnerV1,
    cap: &mut StorageNodeCap,
    epoch: u32,
    clock: &Clock,
) {
    // Make sure the node hasn't attested yet, and set the new epoch as the last sync done epoch.
    assert!(epoch == self.epoch, EInvalidSyncEpoch);
    assert!(cap.last_epoch_sync_done() < self.epoch, EDuplicateSyncDone);
    cap.set_last_epoch_sync_done(self.epoch);

    assert!(self.committee.inner().contains(&cap.node_id()), ENotInCommittee);
    let node_shards = self.committee.shards(&cap.node_id());
    match (self.epoch_state) {
        EpochState::EpochChangeSync(weight) => {
            let weight = weight + (node_shards.length() as u16);
            if (is_quorum(weight, self.n_shards)) {
                self.epoch_state = EpochState::EpochChangeDone(clock.timestamp_ms());
                events::emit_epoch_change_done(self.epoch);
            } else {
                self.epoch_state = EpochState::EpochChangeSync(weight);
            }
        },
        _ => {},
    };
    // Emit the event that the node has received all shards.
    events::emit_shards_received(self.epoch, *node_shards);
}

// === Accessors ===

/// Returns the Option with next committee.
public(package) fun next_committee(self: &StakingInnerV1): &Option<Committee> {
    &self.next_committee
}

/// Returns the next epoch parameters if set, otherwise aborts with an error.
public(package) fun next_epoch_params(self: &StakingInnerV1): EpochParams {
    *self.next_epoch_params.borrow()
}

/// Get the current epoch.
public(package) fun epoch(self: &StakingInnerV1): u32 {
    self.epoch
}

/// Get the current committee.
public(package) fun committee(self: &StakingInnerV1): &Committee {
    &self.committee
}

/// Get the previous committee.
public(package) fun previous_committee(self: &StakingInnerV1): &Committee {
    &self.previous_committee
}

/// Construct the BLS committee for the next epoch.
public(package) fun next_bls_committee(self: &StakingInnerV1): BlsCommittee {
    let (ids, shard_assignments) = (*self.next_committee.borrow().inner()).into_keys_values();
    let members = ids.zip_map!(shard_assignments, |id, shards| {
        let pk = self.pools.borrow(id).node_info().public_key();
        bls_aggregate::new_bls_committee_member(*pk, shards.length() as u16, id)
    });
    bls_aggregate::new_bls_committee(self.epoch + 1, members)
}

/// Check if a node with the given `ID` exists in the staking pools.
public(package) fun has_pool(self: &StakingInnerV1, node_id: ID): bool {
    self.pools.contains(node_id)
}

// === Internal ===

fun new_walrus_context(self: &StakingInnerV1): WalrusContext {
    walrus_context::new(
        self.epoch,
        self.next_committee.is_some(),
        self.committee.to_inner(),
    )
}

fun is_quorum(weight: u16, n_shards: u16): bool {
    3 * (weight as u64) >= 2 * (n_shards as u64) + 1
}

// ==== Tests ===
#[test_only]
use walrus::test_utils::assert_eq;

#[test_only]
public(package) fun is_epoch_sync_done(self: &StakingInnerV1): bool {
    match (self.epoch_state) {
        EpochState::EpochChangeDone(_) => true,
        _ => false,
    }
}

#[test_only]
public(package) fun active_set(self: &mut StakingInnerV1): &mut ActiveSet {
    &mut self.active_set
}

#[test_only]
#[syntax(index)]
/// Get the pool with the given `ID`.
public(package) fun borrow(self: &StakingInnerV1, node_id: ID): &StakingPool {
    &self.pools[node_id]
}

#[test_only]
#[syntax(index)]
/// Get mutable reference to the pool with the given `ID`.
public(package) fun borrow_mut(self: &mut StakingInnerV1, node_id: ID): &mut StakingPool {
    &mut self.pools[node_id]
}

#[test_only]
public(package) fun pub_dhondt(n_shards: u16, stake: vector<u64>): (FixedPoint32, vector<u16>) {
    // TODO better ranking
    let ranking = {
        // TODO use std::vector::tabulate
        let mut v = vector[];
        stake.length().do!(|i| v.push_back(i));
        v
    };
    dhondt(ranking, n_shards, stake)
}

#[test]
fun test_quorum_above() {
    let mut queue = priority_queue::new(vector[]);
    let votes = vector[1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    let weights = vector[5, 5, 4, 6, 3, 7, 2, 8, 1, 9];
    votes.zip_do!(weights, |vote, weight| queue.insert(vote, weight));
    assert_eq!(quorum_above(&mut queue, 50), 4);
}

#[test]
fun test_quorum_above_all_above() {
    let mut queue = priority_queue::new(vector[]);
    let votes = vector[1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    let weights = vector[17, 1, 1, 1, 3, 7, 2, 8, 1, 9];
    votes.zip_do!(weights, |vote, weight| queue.insert(vote, weight));
    assert_eq!(quorum_above(&mut queue, 50), 1);
}

#[test]
fun test_quorum_above_one_value() {
    let mut queue = priority_queue::new(vector[]);
    queue.insert(1, 50);
    assert_eq!(quorum_above(&mut queue, 50), 1);
}

#[test]
fun test_quorum_below() {
    let mut queue = priority_queue::new(vector[]);
    let votes = vector[1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    let weights = vector[5, 5, 4, 6, 3, 7, 4, 6, 1, 9];
    votes.zip_do!(weights, |vote, weight| queue.insert(vote, weight));
    assert_eq!(quorum_below(&mut queue, 50), 7);
}

#[test]
fun test_quorum_below_all_below() {
    let mut queue = priority_queue::new(vector[]);
    let votes = vector[1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    let weights = vector[5, 5, 4, 6, 3, 7, 1, 1, 1, 17];
    votes.zip_do!(weights, |vote, weight| queue.insert(vote, weight));
    assert_eq!(quorum_below(&mut queue, 50), 10);
}

#[test]
fun test_quorum_below_one_value() {
    let mut queue = priority_queue::new(vector[]);
    queue.insert(1, 50);
    assert_eq!(quorum_below(&mut queue, 50), 1);
}
