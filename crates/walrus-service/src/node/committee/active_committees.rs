// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{cmp::Ordering, mem, sync::Arc};

use walrus_core::{ensure, Epoch};
use walrus_sui::types::Committee;

/// Errors returned when starting a committee change with
/// [`ActiveCommittees::begin_committee_change`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum BeginCommitteeChangeError {
    /// Error returned when attempting to start a committee change while one is already in progress.
    #[error("the committees are already changing")]
    ChangeInProgress,
    /// Error returned if the expected next committee has an epoch that is not 1 greater than the
    /// current epoch.
    #[error("the epoch of the new committee is invalid: {actual} (expected {expected})")]
    InvalidEpoch { expected: Epoch, actual: Epoch },
    /// Error returned when the previously set upcoming committee does not match the expected next
    /// committee.
    #[error("the previously set next committee does not match the expected committee")]
    CommitteeMismatch,
}

/// Errors returned when completing a committee change with
/// [`ActiveCommittees::end_committee_change`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum EndCommitteeChangeError {
    /// The current committee change that is in progress was not for the specified epoch.
    #[error("the epoch provided does not match the new epoch: {actual} (expected {expected})")]
    InvalidEpoch { expected: Epoch, actual: Epoch },
    /// Error returned when attempting to end a transition, when none is ongoing.
    #[error("the committee is not currently transitioning")]
    NotTransitioning,
}

/// Errors returned when setting the upcoming committee with
/// [`ActiveCommittees::set_committee_for_next_epoch`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum InvalidNextCommittee {
    /// Error returned when the next committee has already been set.
    #[error("the next committee has already been set")]
    AlreadySet,
    /// Error returned when the epoch of the new committee is not 1 greater than that of the
    /// current committee.
    #[error("the epoch of the new committee is invalid: {actual} (expected {expected})")]
    InvalidEpoch { expected: Epoch, actual: Epoch },
}

/// The current, prior, and next committees in the system.
#[derive(Debug, Clone)]
pub(crate) struct ActiveCommittees {
    /// The currently reigning committee.
    ///
    /// The epoch of the system corresponds to this committee.
    current_committee: Arc<Committee>,

    /// The prior committee.
    ///
    /// When `is_transitioning` is true, this committee continues to serve reads. It is only `None`
    /// when the current epoch is zero.
    // INV: current_committee.epoch == prior_committee.epoch + 1
    // INV: current_committee.epoch == 0 || prior_committee.is_some()
    prior_committee: Option<Arc<Committee>>,

    /// The committee that will become active in the next epoch.
    ///
    /// This committee is `None` until known.
    // INV: upcoming_committee.epoch == current_committee.epoch + 1
    upcoming_committee: Option<Arc<Committee>>,

    /// When true, a committee transition is underway.
    ///
    /// In this case, both the current and prior committee are serving general reads for blobs.
    is_transitioning: bool,
}

impl ActiveCommittees {
    /// Construct a new set of `ActiveCommittees`.
    ///
    /// The prior committee is required for all current_committees beyond epoch 0.
    ///
    /// # Panics
    ///
    /// Panics if the prior committee is None when the current_committee has an epoch greater than
    /// zero, if the prior committee's epoch does not precede that of the current committees, or if
    /// they have a different number of shards.
    pub fn new(current_committee: Committee, prior_committee: Option<Committee>) -> Self {
        let this = Self {
            current_committee: Arc::new(current_committee),
            prior_committee: prior_committee.map(Arc::new),
            upcoming_committee: None,
            is_transitioning: false,
        };
        this.check_invariants();
        this
    }

    /// Construct a new set of `ActiveCommittees`, which are transitioning to the current epoch.
    ///
    /// # Panics
    ///
    /// Panics if the prior committee's epoch does not precede that of the current committees, or if
    /// they have a different number of shards.
    #[cfg(test)]
    pub fn new_transitioning(current_committee: Committee, prior_committee: Committee) -> Self {
        let this = Self {
            current_committee: Arc::new(current_committee),
            prior_committee: Some(Arc::new(prior_committee)),
            upcoming_committee: None,
            is_transitioning: true,
        };
        this.check_invariants();
        this
    }

    /// The current epoch.
    pub fn epoch(&self) -> Epoch {
        self.current_committee.epoch
    }

    /// Returns true if a committee change is in progress, false otherwise.
    pub fn is_change_in_progress(&self) -> bool {
        self.is_transitioning
    }

    /// Returns the current committee.
    pub fn current_committee(&self) -> &Arc<Committee> {
        &self.current_committee
    }

    /// Returns committee currently serving writes.
    ///
    /// This is equivalent to [`Self::current_committee`].
    pub fn write_committee(&self) -> &Arc<Committee> {
        self.current_committee()
    }

    /// Returns the prior committee if not in epoch 0.
    pub fn prior_committee(&self) -> Option<&Arc<Committee>> {
        self.prior_committee.as_ref()
    }

    /// Returns the upcoming committee if known.
    pub fn upcoming_committee(&self) -> Option<&Arc<Committee>> {
        self.upcoming_committee.as_ref()
    }

    /// Returns the committee serving reads for a blob certified in the specified epoch.
    ///
    /// Returns None if the epoch is larger than [`epoch()`][Self::epoch].
    pub fn read_committee(&self, certified_epoch: Epoch) -> Option<&Arc<Committee>> {
        if !self.is_transitioning {
            return Some(&self.current_committee);
        }

        match certified_epoch.cmp(&self.current_committee.epoch) {
            Ordering::Less => {
                Some(self.prior_committee.as_ref().expect(
                    "the committee exists as the current committee does not have epoch zero",
                ))
            }
            Ordering::Equal => Some(&self.current_committee),
            Ordering::Greater => None,
        }
    }

    /// Returns the committee for the specified epoch.
    ///
    /// If the epoch is not the current, prior, or known next epoch, then None is returned.
    pub fn committee_for_epoch(&self, epoch: Epoch) -> Option<&Arc<Committee>> {
        if epoch == self.epoch() {
            return Some(&self.current_committee);
        }
        if epoch == self.epoch() + 1 {
            return self.upcoming_committee.as_ref();
        }
        // if epoch == self.epoch() - 1
        if epoch + 1 == self.epoch() {
            return self.prior_committee.as_ref();
        }
        None
    }

    /// Sets the committee for the next epoch if it has not already been set.
    ///
    /// # Panics
    ///
    /// Panics if the committees have a different number of shards.
    pub fn set_committee_for_next_epoch(
        &mut self,
        committee: Committee,
    ) -> Result<(), InvalidNextCommittee> {
        ensure!(
            self.upcoming_committee.is_none(),
            InvalidNextCommittee::AlreadySet
        );
        ensure!(
            committee.epoch == self.current_committee.epoch + 1,
            InvalidNextCommittee::InvalidEpoch {
                expected: self.current_committee.epoch + 1,
                actual: committee.epoch,
            }
        );

        self.upcoming_committee = Some(Arc::new(committee));
        self.check_invariants();
        Ok(())
    }

    /// Begins the transition of committees that occurs during epoch change.
    ///
    /// If the upcoming committee has not been set, then the expected committee defines the upcoming
    /// committee. Otherwise, the previously set upcoming committee is compared with the expected
    /// committee. If they are the same, then the change can be initiated, otherwise an error is
    /// returned.
    ///
    /// # Panics
    ///
    /// Panics if the expected_committee has a different number of shards.
    pub fn begin_committee_change(
        &mut self,
        expected_committee: Committee,
    ) -> Result<(), BeginCommitteeChangeError> {
        ensure!(
            !self.is_transitioning,
            BeginCommitteeChangeError::ChangeInProgress
        );

        if let Some(ref upcoming_committee) = self.upcoming_committee {
            ensure!(
                **upcoming_committee == expected_committee,
                BeginCommitteeChangeError::CommitteeMismatch
            );
        }

        // Set the upcoming committee if it's not already set.
        match self.set_committee_for_next_epoch(expected_committee) {
            Ok(()) | Err(InvalidNextCommittee::AlreadySet) => (),
            Err(InvalidNextCommittee::InvalidEpoch { expected, actual }) => {
                return Err(BeginCommitteeChangeError::InvalidEpoch { expected, actual })
            }
        }

        let upcoming_committee = self.upcoming_committee.take().expect("set above");
        let prior_committee = mem::replace(&mut self.current_committee, upcoming_committee);
        self.prior_committee = Some(prior_committee);
        self.is_transitioning = true;

        self.check_invariants();

        debug_assert!(self.is_transitioning);
        Ok(())
    }

    /// Completes the transition of a committee from the old epoch to the new.
    ///
    /// The expected epoch must match with the epoch of the newest committee, that is, the new,
    /// current epoch. If not, then an error is returned and the committee change is not completed.
    ///
    /// On completion, returns a reference to the outgoing committee: the new prior committee.
    pub fn end_committee_change(
        &mut self,
        expected_epoch: Epoch,
    ) -> Result<&Arc<Committee>, EndCommitteeChangeError> {
        ensure!(
            self.is_transitioning,
            EndCommitteeChangeError::NotTransitioning
        );
        ensure!(
            self.epoch() == expected_epoch,
            EndCommitteeChangeError::InvalidEpoch {
                expected: expected_epoch,
                actual: self.epoch()
            }
        );

        self.is_transitioning = false;
        let prior_committee = self
            .prior_committee
            .as_ref()
            .expect("there is always a prior committee after a transition");

        self.check_invariants();
        Ok(prior_committee)
    }

    fn check_invariants(&self) {
        assert!(
            self.current_committee.epoch == 0 || self.prior_committee.is_some(),
            "prior committee must be set for non-genesis epochs"
        );

        if let Some(ref prior_committee) = self.prior_committee {
            assert_eq!(
                self.current_committee.epoch,
                prior_committee.epoch + 1,
                "the current committee's epoch must be one more than the prior's"
            );
            assert_eq!(
                self.current_committee.n_shards(),
                prior_committee.n_shards(),
                "the current committee and prior committees must have the same number of shards"
            );
        }

        if let Some(ref upcoming_committee) = self.upcoming_committee {
            assert_eq!(
                self.current_committee.epoch + 1,
                upcoming_committee.epoch,
                "the upcoming committee's epoch must be one more than the current's"
            );
            assert_eq!(
                self.current_committee.n_shards(),
                upcoming_committee.n_shards(),
                "the current committee and prior committees must have the same number of shards"
            );
        }
    }
}
