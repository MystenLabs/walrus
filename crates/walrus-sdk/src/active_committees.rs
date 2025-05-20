// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Active Committees.
use std::{cmp::Ordering, collections::HashSet, mem, num::NonZeroU16, sync::Arc};

use walrus_core::{Epoch, NetworkPublicKey, ensure};
use walrus_sui::{
    client::CommitteesAndState,
    types::{Committee, NetworkAddress},
};

/// The current, previous, and next committees in the system.
// INV: current_committee.n_shards() == previous_committee.n_shards() == next_committee.n_shards()
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActiveCommittees {
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
        current_committee: Arc<Committee>,
        previous_committee: Option<Arc<Committee>>,
        next_committee: Option<Arc<Committee>>,
        is_transitioning: bool,
    ) -> Self {
        let this = Self {
            current_committee,
            previous_committee,
            next_committee,
            is_transitioning,
        };
        this.check_invariants();
        this
    }

    /// Construct a new set of `ActiveCommittees` from the [`CommitteesAndState`] returned by the
    /// [`walrus_sui::client::ReadClient`].
    ///
    /// # Panics
    ///
    /// Panics if the previous committee is None when the current_committee has an epoch greater
    /// than zero, if the previous committee's epoch does not precede that of the current
    /// committees, or if they have a different number of shards.
    #[tracing::instrument(skip_all)]
    pub fn from_committees_and_state(committees_and_state: CommitteesAndState) -> Self {
        Self::try_from(committees_and_state).expect("ActiveCommittees invariants must be upheld")
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
        if certified_epoch > self.current_committee.epoch {
            return None;
        }

        if !self.is_transitioning {
            return Some(&self.current_committee);
        }

        match certified_epoch.cmp(&self.current_committee.epoch) {
            Ordering::Less => self.previous_committee.as_ref(),
            Ordering::Equal => Some(&self.current_committee),
            Ordering::Greater => unreachable!("certified_epoch > current_epoch checked above"),
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

    // Functions that rely on the fact that `n_shards` is the same for all committees.

    /// Returns the number of shards in the committee.
    ///
    /// Given the invariants enforced by this struct, `n_shards` is the same for all committees.
    pub fn n_shards(&self) -> NonZeroU16 {
        self.current_committee.n_shards()
    }

    fn try_check_invariants(&self) -> Result<(), anyhow::Error> {
        ensure!(
            self.current_committee.epoch == 0 || self.previous_committee.is_some(),
            "previous committee must be set for non-genesis epochs"
        );

        if let Some(ref previous_committee) = self.previous_committee {
            ensure!(
                self.current_committee.epoch == previous_committee.epoch + 1,
                "the current committee's epoch must be one more than the previous's"
            );
            ensure!(
                self.current_committee.n_shards() == previous_committee.n_shards(),
                "the current committee and previous committees must have the same number of shards"
            );
        }

        if let Some(ref next_committee) = self.next_committee {
            ensure!(
                self.current_committee.epoch + 1 == next_committee.epoch,
                "the next committee's epoch must be one more than the current's"
            );
            ensure!(
                self.current_committee.n_shards() == next_committee.n_shards(),
                "the current committee and previous committees must have the same number of shards"
            );
        }

        Ok(())
    }

    fn check_invariants(&self) {
        self.try_check_invariants()
            .expect("ActiveCommittee's invariants must be upheld");
    }

    /// Checks if the number is larger or equal to the minimum number of correct shards.
    ///
    /// Given the invariants enforced by this struct, the result of this function is the same for
    /// all committees.
    ///
    /// See [`min_n_correct`][Self::min_n_correct] for further details.
    #[inline]
    #[allow(dead_code)]
    pub fn is_at_least_min_n_correct(&self, num: usize) -> bool {
        self.current_committee.is_at_least_min_n_correct(num)
    }

    /// Returns the minimum number of correct shards.
    ///
    /// Given the invariants enforced by this struct, the result of this function is the same for
    /// all committees.
    ///
    /// This is (`n_shards - f`), where `f` is the maximum number of faulty shards, given
    /// `n_shards`. See [walrus_core::bft] for further details.
    #[inline]
    #[allow(dead_code)]
    pub fn min_n_correct(&self) -> usize {
        self.current_committee.min_n_correct()
    }

    /// Checks if the number is large enough to reach a quorum (`2f + 1`).
    ///
    /// Given the invariants enforced by this struct, the result of this function is the same for
    /// all committees.
    ///
    /// `f` is the maximum number of faulty shards, given `n_shards`. See [walrus_core::bft] for
    /// further details.
    #[inline]
    pub fn is_quorum(&self, num: usize) -> bool {
        self.current_committee.is_quorum(num)
    }

    /// Checks if the number is larger or equal to the validity threshold
    ///
    ///
    /// Given the invariants enforced by this struct, the result of this function is the same for
    /// all committees.
    ///
    /// The validity threshold is `f + 1`, where `f` is the maximum number of faulty shards. See
    /// [walrus_core::bft] for further details.
    #[inline]
    #[allow(dead_code)]
    pub fn is_above_validity(&self, num: usize) -> bool {
        self.current_committee.is_above_validity(num)
    }

    /// Returns the set of unique (network address, network public key) pairs in the committees.
    ///
    /// Each storage node is uniquely identified by this pair. The function returns the union over
    /// the pairs of the members in previous, current, and next, committees.
    #[allow(clippy::mutable_key_type)]
    pub fn unique_node_address_and_key(&self) -> HashSet<(&NetworkAddress, &NetworkPublicKey)> {
        let mut members = HashSet::from_iter(self.current_committee.network_addresses_and_pks());
        if let Some(previous) = self.previous_committee() {
            members.extend(previous.network_addresses_and_pks());
        }
        if let Some(next) = self.next_committee() {
            members.extend(next.network_addresses_and_pks());
        }
        members
    }
}

impl TryFrom<CommitteesAndState> for ActiveCommittees {
    type Error = anyhow::Error;

    fn try_from(committees_and_state: CommitteesAndState) -> Result<Self, Self::Error> {
        let this = Self {
            current_committee: Arc::new(committees_and_state.current),
            previous_committee: committees_and_state.previous.map(Arc::new),
            next_committee: committees_and_state.next.map(Arc::new),
            is_transitioning: committees_and_state.epoch_state.is_transitioning(),
        };
        this.try_check_invariants()?;
        Ok(this)
    }
}

/// Errors returned when the next committee is inconsistent with the provided committee.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub struct NextCommitteeInconsistent(String);

impl std::fmt::Display for NextCommitteeInconsistent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Errors returned when starting a committee change.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum StartChangeError {
    /// Error returned when the next committee is not known.
    #[error("the next committee is unknown")]
    UnknownNextCommittee,
    /// Error returned when attempting to start a committee change while one is already in progress.
    #[error("the committees are already changing")]
    ChangeInProgress,
}

/// Errors returned when ending a committee change.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("cannot end a committee change as it is not in progress")]
pub struct ChangeNotInProgress;

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

    /// The next epoch to which this committee would be transitioning.
    pub fn next_epoch(&self) -> Epoch {
        self.0.epoch() + 1
    }

    /// The epoch of the committee that is currently being tracked.
    pub fn tracked_committee_epoch(&self) -> Epoch {
        self.0.epoch()
    }

    /// Sets the committee for the next epoch, always update the next committee with the
    /// provided committee.
    ///
    /// # Panics
    ///
    /// Panics if the committees have a different number of shards, or if the epoch of the provided
    /// committee does not match [`Self::next_epoch()`], or the shard assignment is different.
    pub fn set_committee_for_next_epoch(
        &mut self,
        committee: Committee,
    ) -> Result<(), NextCommitteeInconsistent> {
        assert_eq!(
            committee.epoch,
            self.next_epoch(),
            "committee's epoch must match the next epoch"
        );
        if let Some(next_committee) = self.0.next_committee.as_ref() {
            next_committee
                .compare_essential(&committee)
                .map_err(|e| NextCommitteeInconsistent(e.to_string()))?;
        }

        self.0.next_committee = Some(Arc::new(committee));
        self.0.check_invariants();
        Ok(())
    }

    /// Begins tracking the transition to `next_committee`.
    ///
    /// The next committee should already be set, and a change should not already be in progress.
    pub fn start_change(&mut self) -> Result<(), StartChangeError> {
        ensure!(
            self.0.next_committee.is_some(),
            StartChangeError::UnknownNextCommittee,
        );
        ensure!(!self.0.is_transitioning, StartChangeError::ChangeInProgress);

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
    /// Returns an error if the change is not currently in progress.
    ///
    /// On completion, returns a reference to the outgoing committee: the new previous committee.
    pub fn end_change(&mut self) -> Result<&Arc<Committee>, ChangeNotInProgress> {
        ensure!(self.0.is_transitioning, ChangeNotInProgress);

        self.0.is_transitioning = false;
        let previous_committee = self
            .0
            .previous_committee
            .as_ref()
            .expect("there is always a previous committee after a transition");

        self.0.check_invariants();
        debug_assert!(!self.0.is_transitioning);
        Ok(previous_committee)
    }

    /// Updates the active committees with the provided `ActiveCommittees`.
    pub fn update_active_committees(&mut self, active_committees: ActiveCommittees) {
        self.0 = active_committees;
    }
}

impl From<ActiveCommittees> for CommitteeTracker {
    fn from(active_committees: ActiveCommittees) -> Self {
        Self::new(active_committees)
    }
}
