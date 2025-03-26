// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use std::{
    fmt::{Debug, Display},
    time::Duration,
};

use serde::{Deserialize, Serialize};

use crate::client::Instance;

#[derive(Clone, Serialize, Deserialize, Hash, PartialEq, Eq)]
pub enum FaultsType {
    /// Permanently crash the maximum number of nodes from the beginning.
    Permanent { faults: usize },
    /// Progressively crash and recover nodes.
    CrashRecovery {
        max_faults: usize,
        interval: Duration,
    },
}

impl Default for FaultsType {
    fn default() -> Self {
        Self::Permanent { faults: 0 }
    }
}

impl Debug for FaultsType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Permanent { faults } => write!(f, "{faults}"),
            Self::CrashRecovery {
                max_faults,
                interval,
            } => write!(f, "{max_faults}-{}cr", interval.as_secs()),
        }
    }
}

impl Display for FaultsType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Permanent { faults } => {
                if *faults == 0 {
                    write!(f, "no faults")
                } else {
                    write!(f, "{faults} crashed")
                }
            }
            Self::CrashRecovery {
                max_faults,
                interval,
            } => write!(f, "{max_faults} crash-recovery, {}s", interval.as_secs()),
        }
    }
}

impl FaultsType {
    /// The interval between crashes. If the type is `Permanent`, the interval is 1s
    /// to crash the nodes as fast as possible.
    pub fn crash_interval(&self) -> Duration {
        match self {
            Self::Permanent { .. } => Duration::from_secs(1),
            Self::CrashRecovery { interval, .. } => *interval,
        }
    }
}

/// The actions to apply to the testbed, i.e., which instances to crash and recover.
#[derive(Default)]
pub struct CrashRecoveryAction {
    /// The instances to boot.
    pub boot: Vec<Instance>,
    /// The instances to kill.
    pub kill: Vec<Instance>,
}

impl Display for CrashRecoveryAction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let booted = self.boot.len();
        let killed = self.kill.len();

        if self.boot.is_empty() {
            write!(f, "{killed} node(s) killed")
        } else if self.kill.is_empty() {
            write!(f, "{booted} node(s) recovered")
        } else {
            write!(f, "{killed} node(s) killed and {booted} node(s) recovered")
        }
    }
}

impl CrashRecoveryAction {
    pub fn boot(instances: impl Iterator<Item = Instance>) -> Self {
        Self {
            boot: instances.collect(),
            kill: Vec::new(),
        }
    }

    pub fn kill(instances: impl Iterator<Item = Instance>) -> Self {
        Self {
            boot: Vec::new(),
            kill: instances.collect(),
        }
    }

    pub fn no_op() -> Self {
        Self::default()
    }
}

pub struct CrashRecoverySchedule {
    /// The number of faulty nodes and the crash-recovery pattern to follow.
    faults_type: FaultsType,
    /// The available instances.
    instances: Vec<Instance>,
    /// The current number of dead nodes.
    dead: usize,
}

impl CrashRecoverySchedule {
    pub fn new(faults_type: FaultsType, instances: Vec<Instance>) -> Self {
        Self {
            faults_type,
            instances,
            dead: 0,
        }
    }
    pub fn update(&mut self) -> CrashRecoveryAction {
        let mut instances = self.instances.clone();

        match &self.faults_type {
            // Permanently crash the specified number of nodes.
            FaultsType::Permanent { faults } => {
                if self.dead == 0 {
                    self.dead = *faults;
                    CrashRecoveryAction::kill(instances.drain(0..*faults))
                } else {
                    CrashRecoveryAction::no_op()
                }
            }

            // Periodically crash and recover nodes.
            FaultsType::CrashRecovery { max_faults, .. } => {
                let min_faults = max_faults / 3;

                // Recover all nodes if we already crashed them all.
                if self.dead == *max_faults {
                    let to_recover = instances.drain(0..*max_faults);
                    self.dead = 0;
                    CrashRecoveryAction::boot(to_recover)
                }
                // Otherwise crash a few nodes at the time.
                else {
                    let (l, h) = if self.dead == 0 && min_faults != 0 {
                        (0, min_faults)
                    } else if self.dead == min_faults && min_faults != 0 {
                        (min_faults, 2 * min_faults)
                    } else {
                        (2 * min_faults, *max_faults)
                    };

                    let to_kill = instances.drain(l..h);
                    self.dead += h - l;
                    CrashRecoveryAction::kill(to_kill)
                }
            }
        }
    }
}

#[cfg(test)]
mod faults_tests {
    use std::time::Duration;

    use super::{CrashRecoverySchedule, FaultsType};
    use crate::client::Instance;

    #[test]
    fn crash_recovery_1_fault() {
        let max_faults = 1;
        let interval = Duration::from_secs(60);
        let faulty = (0..max_faults)
            .map(|i| Instance::new_for_test(i.to_string()))
            .collect();
        let mut schedule = CrashRecoverySchedule::new(
            FaultsType::CrashRecovery {
                max_faults,
                interval,
            },
            faulty,
        );

        let action = schedule.update();
        assert_eq!(action.boot.len(), 0);
        assert_eq!(action.kill.len(), 1);

        let action = schedule.update();
        assert_eq!(action.boot.len(), 1);
        assert_eq!(action.kill.len(), 0);

        let action = schedule.update();
        assert_eq!(action.boot.len(), 0);
        assert_eq!(action.kill.len(), 1);

        let action = schedule.update();
        assert_eq!(action.boot.len(), 1);
        assert_eq!(action.kill.len(), 0);
    }

    #[test]
    fn crash_recovery_2_faults() {
        let max_faults = 2;
        let interval = Duration::from_secs(60);
        let faulty = (0..max_faults)
            .map(|i| Instance::new_for_test(i.to_string()))
            .collect();
        let mut schedule = CrashRecoverySchedule::new(
            FaultsType::CrashRecovery {
                max_faults,
                interval,
            },
            faulty,
        );

        let action = schedule.update();
        assert_eq!(action.boot.len(), 0);
        assert_eq!(action.kill.len(), 2);

        let action = schedule.update();
        assert_eq!(action.boot.len(), 2);
        assert_eq!(action.kill.len(), 0);

        let action = schedule.update();
        assert_eq!(action.boot.len(), 0);
        assert_eq!(action.kill.len(), 2);

        let action = schedule.update();
        assert_eq!(action.boot.len(), 2);
        assert_eq!(action.kill.len(), 0);
    }

    #[test]
    fn crash_recovery() {
        let interval = Duration::from_secs(60);
        for i in 3..33 {
            let max_faults = i;
            let min_faults = max_faults / 3;

            let instances = (0..max_faults)
                .map(|i| Instance::new_for_test(i.to_string()))
                .collect();
            let mut schedule = CrashRecoverySchedule::new(
                FaultsType::CrashRecovery {
                    max_faults,
                    interval,
                },
                instances,
            );

            let action = schedule.update();
            assert_eq!(action.boot.len(), 0);
            assert_eq!(action.kill.len(), min_faults);

            let action = schedule.update();
            assert_eq!(action.boot.len(), 0);
            assert_eq!(action.kill.len(), min_faults);

            let action = schedule.update();
            assert_eq!(action.boot.len(), 0);
            assert_eq!(action.kill.len(), max_faults - 2 * min_faults);

            let action = schedule.update();
            assert_eq!(action.boot.len(), max_faults);
            assert_eq!(action.kill.len(), 0);

            let action = schedule.update();
            assert_eq!(action.boot.len(), 0);
            assert_eq!(action.kill.len(), min_faults);
        }
    }
}
