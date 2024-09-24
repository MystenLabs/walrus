// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{cmp::Ordering, mem, num::NonZeroU16, sync::Arc};

use walrus_core::{ensure, Epoch};
use walrus_sui::{client::CommitteesAndState, types::Committee};

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
    /// Error returned when the previously set next committee does not match the expected next
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

/// Errors returned when setting the next committee with
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

/// The current, previous, and next committees in the system.
// INV: current_committee.n_shards() == previous_committee.n_shards() == next_committee.n_shards()
#[derive(Debug, Clone)]
pub(crate) struct ActiveCommittees {
    /// The currently reigning committee.
    ///
    /// The epoch of the system corresponds to this committee.
    current_committee: Arc<Committee>,

    /// The previous committee.
    ///
    /// When `is_transitioning` is true, this committee continues to serve reads. It is only `None`
    /// when the current epoch is zero.
    // INV: current_committee.epoch == previous_committee.epoch + 1
    // INV: current_committee.epoch == 0 || previous_committee.is_some()
    previous_committee: Option<Arc<Committee>>,

    /// The committee that will become active in the next epoch.
    ///
    /// This committee is `None` until known.
    // INV: next_committee.epoch == current_committee.epoch + 1
    next_committee: Option<Arc<Committee>>,

    /// When true, a committee transition is underway.
    ///
    /// In this case, both the current and previous committee are serving general reads for blobs.
    is_transitioning: bool,
}

impl ActiveCommittees {
    /// Construct a new set of `ActiveCommittees`.
    ///
    /// The previous committee is required for all current_committees beyond epoch 0.
    ///
    /// # Panics
    ///
    /// Panics if the previous committee is None when the current_committee has an epoch greater
    /// than zero, if the previous committee's epoch does not precede that of the current
    /// committees, or if they have a different number of shards.
    pub fn new(current_committee: Committee, previous_committee: Option<Committee>) -> Self {
        let this = Self {
            current_committee: Arc::new(current_committee),
            previous_committee: previous_committee.map(Arc::new),
            next_committee: None,
            is_transitioning: false,
        };
        this.check_invariants();
        this
    }

    /// Construct a new set of `ActiveCommittees`, which are transitioning to the current epoch.
    ///
    /// # Panics
    ///
    /// Panics if the previous committee's epoch does not precede that of the current committees, or
    /// if they have a different number of shards.
    #[cfg(test)]
    pub fn new_transitioning(current_committee: Committee, previous_committee: Committee) -> Self {
        let this = Self {
            current_committee: Arc::new(current_committee),
            previous_committee: Some(Arc::new(previous_committee)),
            next_committee: None,
            is_transitioning: true,
        };
        this.check_invariants();
        this
    }

    /// Construct a new set of `ActiveCommittees`, allowing to set all fields.
    ///
    /// The previous committee is required for all current_committees beyond epoch 0.
    ///
    /// # Panics
    ///
    /// Panics if the previous committee is None when the current_committee has an epoch greater
    /// than zero, if the previous committee's epoch does not precede that of the current
    /// committees, or if they have a different number of shards.
    pub fn new_with_next(
        current_committee: Committee,
        previous_committee: Option<Committee>,
        next_committee: Option<Committee>,
        is_transitioning: bool,
    ) -> Self {
        let this = Self {
            current_committee: Arc::new(current_committee),
            previous_committee: previous_committee.map(Arc::new),
            next_committee: next_committee.map(Arc::new),
            is_transitioning,
        };
        this.check_invariants();
        this
    }

    /// Construct a new set of `ActiveCommittees` from the [`CommitteesAndState`] returned by the
    /// [`ReadClient`].
    ///
    /// # Panics
    ///
    /// Panics if the previous committee is None when the current_committee has an epoch greater
    /// than zero, if the previous committee's epoch does not precede that of the current
    /// committees, or if they have a different number of shards.
    #[tracing::instrument(skip_all)]
    pub fn from_committees_and_state(committees_and_state: CommitteesAndState) -> Self {
        Self::new_with_next(
            committees_and_state.current,
            committees_and_state.previous,
            committees_and_state.next,
            committees_and_state.epoch_state.is_transitioning(),
        )
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

    /// Returns the previous committee if not in epoch 0.
    pub fn previous_committee(&self) -> Option<&Arc<Committee>> {
        self.previous_committee.as_ref()
    }

    /// Returns the next committee if known.
    pub fn next_committee(&self) -> Option<&Arc<Committee>> {
        self.next_committee.as_ref()
    }

    /// Returns the committee serving reads for a blob certified in the specified epoch.
    ///
    /// Returns None if the epoch is larger than [`epoch()`][Self::epoch].
    pub fn read_committee(&self, certified_epoch: Epoch) -> Option<&Arc<Committee>> {
        if !self.is_transitioning {
            return Some(&self.current_committee);
        }

        match certified_epoch.cmp(&self.current_committee.epoch) {
            Ordering::Less => self.previous_committee.as_ref(),
            Ordering::Equal => Some(&self.current_committee),
            Ordering::Greater => None,
        }
    }

    /// Returns the committee for the specified epoch.
    ///
    /// If the epoch is not the current, previous, or known next epoch, then None is returned.
    pub fn committee_for_epoch(&self, epoch: Epoch) -> Option<&Arc<Committee>> {
        if epoch == self.epoch() {
            return Some(&self.current_committee);
        }
        if epoch == self.epoch() + 1 {
            return self.next_committee.as_ref();
        }
        // if epoch == self.epoch() - 1
        if epoch + 1 == self.epoch() {
            return self.previous_committee.as_ref();
        }
        None
    }

    /// Returns the number of shards in the committee.
    ///
    /// Given the invariants enforced by this struct, `n_shards` is the same for all committees.
    pub fn n_shards(&self) -> NonZeroU16 {
        self.current_committee.n_shards()
    }

    fn check_invariants(&self) {
        assert!(
            self.current_committee.epoch == 0 || self.previous_committee.is_some(),
            "previous committee must be set for non-genesis epochs"
        );

        if let Some(ref previous_committee) = self.previous_committee {
            assert_eq!(
                self.current_committee.epoch,
                previous_committee.epoch + 1,
                "the current committee's epoch must be one more than the previous's"
            );
            assert_eq!(
                self.current_committee.n_shards(),
                previous_committee.n_shards(),
                "the current committee and previous committees must have the same number of shards"
            );
        }

        if let Some(ref next_committee) = self.next_committee {
            assert_eq!(
                self.current_committee.epoch + 1,
                next_committee.epoch,
                "the next committee's epoch must be one more than the current's"
            );
            assert_eq!(
                self.current_committee.n_shards(),
                next_committee.n_shards(),
                "the current committee and previous committees must have the same number of shards"
            );
        }
    }
}

/// Track committee changes on top [`ActiveCommittees`].
#[derive(Debug)]
pub struct CommitteeTracker(ActiveCommittees);

impl CommitteeTracker {
    /// Constructs a new instance of [`CommitteeTracker`].
    pub fn new(active_committees: ActiveCommittees) -> Self {
        Self(active_committees)
    }

    /// Returns the inner [`ActiveCommittees`] instance.
    pub fn committees(&self) -> &ActiveCommittees {
        &self.0
    }

    /// Returns true if a committee change is in progress, false otherwise.
    pub fn is_change_in_progress(&self) -> bool {
        self.0.is_change_in_progress()
    }

    /// Sets the committee for the next epoch if it has not already been set.
    ///
    /// # Panics
    ///
    /// Panics if the committees have a different number of shards.
    pub(crate) fn set_committee_for_next_epoch(
        &mut self,
        committee: Committee,
    ) -> Result<(), InvalidNextCommittee> {
        ensure!(
            self.0.next_committee.is_none(),
            InvalidNextCommittee::AlreadySet
        );
        ensure!(
            committee.epoch == self.0.current_committee.epoch + 1,
            InvalidNextCommittee::InvalidEpoch {
                expected: self.0.current_committee.epoch + 1,
                actual: committee.epoch,
            }
        );

        self.0.next_committee = Some(Arc::new(committee));
        self.0.check_invariants();
        Ok(())
    }

    /// Begins the transition of committees that occurs during epoch change.
    ///
    /// If the next committee has not been set, then the expected committee defines the next
    /// committee. Otherwise, the previously set next committee is compared with the expected
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
            !self.0.is_transitioning,
            BeginCommitteeChangeError::ChangeInProgress
        );

        if let Some(ref next_committee) = self.0.next_committee {
            ensure!(
                **next_committee == expected_committee,
                BeginCommitteeChangeError::CommitteeMismatch
            );
        }

        // Set the next committee if it's not already set.
        match self.set_committee_for_next_epoch(expected_committee) {
            Ok(()) | Err(InvalidNextCommittee::AlreadySet) => (),
            Err(InvalidNextCommittee::InvalidEpoch { expected, actual }) => {
                return Err(BeginCommitteeChangeError::InvalidEpoch { expected, actual })
            }
        }

        let next_committee = self.0.next_committee.take().expect("set above");
        let previous_committee = mem::replace(&mut self.0.current_committee, next_committee);
        self.0.previous_committee = Some(previous_committee);
        self.0.is_transitioning = true;

        self.0.check_invariants();

        debug_assert!(self.0.is_transitioning);
        Ok(())
    }

    /// Completes the transition of a committee from the old epoch to the new.
    ///
    /// The expected epoch must match with the epoch of the newest committee, that is, the new,
    /// current epoch. If not, then an error is returned and the committee change is not completed.
    ///
    /// On completion, returns a reference to the outgoing committee: the new previous committee.
    pub fn end_committee_change(
        &mut self,
        expected_epoch: Epoch,
    ) -> Result<&Arc<Committee>, EndCommitteeChangeError> {
        ensure!(
            self.0.is_transitioning,
            EndCommitteeChangeError::NotTransitioning
        );
        ensure!(
            self.0.epoch() == expected_epoch,
            EndCommitteeChangeError::InvalidEpoch {
                expected: expected_epoch,
                actual: self.0.epoch()
            }
        );

        self.0.is_transitioning = false;
        let previous_committee = self
            .0
            .previous_committee
            .as_ref()
            .expect("there is always a previous committee after a transition");

        self.0.check_invariants();
        Ok(previous_committee)
    }
}

impl From<ActiveCommittees> for CommitteeTracker {
    fn from(active_committees: ActiveCommittees) -> Self {
        Self::new(active_committees)
    }
}
