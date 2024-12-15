// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/// Module: upgrade
module walrus::upgrade;

use sui::package::{UpgradeCap, UpgradeTicket, UpgradeReceipt};
use walrus::{staking::Staking, system::System};

const EInvalidUpgradeManager: u64 = 0;

public struct UpgradeManager has key {
    id: UID,
    cap: UpgradeCap,
}

public struct EmergencyUpgradeCap has key, store {
    id: UID,
    upgrade_manager_id: ID,
}

public(package) fun new(cap: UpgradeCap, ctx: &mut TxContext): EmergencyUpgradeCap {
    let upgrade_manager = UpgradeManager { id: object::new(ctx), cap };
    let emergency_upgrade_cap = EmergencyUpgradeCap {
        id: object::new(ctx),
        upgrade_manager_id: object::id(&upgrade_manager),
    };
    transfer::share_object(upgrade_manager);
    emergency_upgrade_cap
}

/// Authorize an upgrade using the emergency upgrade cap.
///
/// This should be used sparingly and once walrus has a healthy community and governance,
/// the EmergencyUpgradeCap should be burned.
public fun authorize_emergency_upgrade(
    upgrade_manager: &mut UpgradeManager,
    emergency_upgrade_cap: &EmergencyUpgradeCap,
    digest: vector<u8>,
): UpgradeTicket {
    assert!(
        emergency_upgrade_cap.upgrade_manager_id == object::id(upgrade_manager),
        EInvalidUpgradeManager,
    );
    let policy = upgrade_manager.cap.policy();
    upgrade_manager.cap.authorize(policy, digest)
}

/// Commits an upgrade and sets the new package id in the staking and system objects.
///
/// After committing an upgrade, the staking and system objects should be migrated
/// using the [`package::migrate`] function to emit an event that informs all storage nodes
/// and prevent previous package versions from being used.
public fun commit_upgrade(
    upgrade_manager: &mut UpgradeManager,
    staking: &mut Staking,
    system: &mut System,
    receipt: UpgradeReceipt,
) {
    let new_package_id = receipt.package();
    staking.set_new_package_id(new_package_id);
    system.set_new_package_id(new_package_id);
    upgrade_manager.cap.commit(receipt)
}

/// Burn the emergency upgrade cap.
///
/// This will prevent any further upgrades using the `EmergencyUpgradeCap` and will
/// make upgrades fully reliant on quorum-based governance.
public fun burn_emergency_upgrade_cap(emergency_upgrade_cap: EmergencyUpgradeCap) {
    let EmergencyUpgradeCap { id, .. } = emergency_upgrade_cap;
    id.delete();
}
