// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

module walrus::e2e_runner;

use sui::{clock::{Self, Clock}, test_scenario::{Self, Scenario}, test_utils};
use walrus::{init, staking::Staking, system::System};

const MAX_EPOCHS_AHEAD: u32 = 104;

// === Tests Runner ===

/// The test runner for end-to-end tests.
public struct TestRunner {
    scenario: Scenario,
    clock: Clock,
    admin: address,
}

/// Add any parameters to the initialization, such as epoch zero duration and number of shards.
/// They will be used by the e2e runner admin during the initialization.
public struct InitBuilder {
    epoch_zero_duration: Option<u64>,
    epoch_duration: Option<u64>,
    n_shards: Option<u16>,
    admin: address,
}

/// Prepare the test runner with the given admin address. Returns a builder to
/// set optional parameters: `epoch_zero_duration` and `n_shards`.
///
/// Example:
/// ```move
/// let admin = 0xA11CE;
/// let mut runner = e2e_runner::prepare(admin)
///    .epoch_zero_duration(100000000)
///    .n_shards(100)
///    .build();
///
/// runner.tx!(admin, |staking, system, ctx| { /* ... */ });
/// ```
public fun prepare(admin: address): InitBuilder {
    InitBuilder {
        epoch_zero_duration: option::none(),
        epoch_duration: option::none(),
        n_shards: option::none(),
        admin,
    }
}

/// Change the epoch zero duration.
public fun epoch_zero_duration(mut self: InitBuilder, duration: u64): InitBuilder {
    self.epoch_zero_duration = option::some(duration);
    self
}

/// Change the regular (non-zero) epoch duration.
public fun epoch_duration(mut self: InitBuilder, duration: u64): InitBuilder {
    self.epoch_duration = option::some(duration);
    self
}

/// Change the number of shards in the system.
public fun n_shards(mut self: InitBuilder, n: u16): InitBuilder {
    self.n_shards = option::some(n);
    self
}

/// Build the test runner with the given parameters.
public fun build(self: InitBuilder): TestRunner {
    let InitBuilder { admin, epoch_duration, epoch_zero_duration, n_shards } = self;
    let epoch_zero_duration = epoch_zero_duration.destroy_or!(100000000);
    let epoch_duration = epoch_duration.destroy_or!(7 * 24 * 60 * 60 * 1000 / 2);
    let n_shards = n_shards.destroy_or!(100);

    let mut scenario = test_scenario::begin(admin);
    let clock = clock::create_for_testing(scenario.ctx());
    let ctx = scenario.ctx();

    init::init_for_testing(ctx);

    scenario.next_tx(admin);
    let cap = scenario.take_from_sender<init::InitCap>();
    let ctx = scenario.ctx();
    init::initialize_walrus(
        cap,
        epoch_zero_duration,
        epoch_duration,
        n_shards,
        MAX_EPOCHS_AHEAD,
        &clock,
        ctx,
    );

    TestRunner { scenario, clock, admin }
}

/// Get the admin address that published Walrus System and Staking.
public fun admin(self: &TestRunner): address { self.admin }

/// Access runner's `Scenario`.
public fun scenario(self: &mut TestRunner): &mut Scenario { &mut self.scenario }

/// Access runner's `Clock`.
public fun clock(self: &mut TestRunner): &mut Clock { &mut self.clock }

/// Access the current epoch of the system.
public fun epoch(self: &mut TestRunner): u32 {
    self.scenario.next_tx(self.admin);
    let system = self.scenario.take_shared<System>();
    let epoch = system.epoch();
    test_scenario::return_shared(system);
    epoch
}

/// Run a transaction as a `sender`, and call the function `f` with the `Staking`,
/// `System` and `TxContext` as arguments.
public macro fun tx(
    $runner: &mut TestRunner,
    $sender: address,
    $f: |&mut Staking, &mut System, &mut TxContext|,
) {
    let runner = $runner;
    let scenario = runner.scenario();
    scenario.next_tx($sender);
    let mut staking = scenario.take_shared<Staking>();
    let mut system = scenario.take_shared<System>();
    let ctx = scenario.ctx();

    $f(&mut staking, &mut system, ctx);

    test_scenario::return_shared(staking);
    test_scenario::return_shared(system);
}

/// Destroy the test runner and all resources.
public fun destroy(self: TestRunner) {
    test_utils::destroy(self)
}
