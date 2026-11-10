// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

module walrus::init_tests;

use std::unit_test::destroy;
use sui::{clock, package, test_scenario};
use walrus::init;

#[test, expected_failure(abort_code = init::EDeprecatedFunction)]
fun initialize_walrus_is_deprecated() {
    let admin = @0xA11CE;
    let mut scenario = test_scenario::begin(admin);
    init::init_for_testing(scenario.ctx());
    // We need an upgrade cap for package with address 0x0
    let package_id = scenario.ctx().fresh_object_address().to_id();
    let upgrade_cap = package::test_publish(package_id, scenario.ctx());
    scenario.next_tx(admin);
    let init_cap = scenario.take_from_sender<init::InitCap>();
    let clock = clock::create_for_testing(scenario.ctx());
    let emergency_upgrade_cap = init::initialize_walrus(
        init_cap,
        upgrade_cap,
        100000000,
        1000000,
        1000,
        53,
        &clock,
        scenario.ctx(),
    );
    destroy(emergency_upgrade_cap);
    clock.destroy_for_testing();
    scenario.end();
}
