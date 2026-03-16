// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Permanent blob info types and operations.

use std::num::NonZeroU32;

use serde::{Deserialize, Serialize};
use sui_types::event::EventID;
use walrus_core::Epoch;

use super::BlobStatusChangeInfo;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct PermanentBlobInfo {
    /// The total number of `Blob` objects for that blob ID with the given status.
    pub count: NonZeroU32,
    /// The latest expiration epoch among these objects.
    pub end_epoch: Epoch,
    /// The ID of the first blob event that led to the status with the given `end_epoch`.
    pub event: EventID,
}

impl PermanentBlobInfo {
    /// Creates a new `PermanentBlobInfo` object for the first blob with the given `end_epoch` and
    /// `event`.
    pub(super) fn new_first(end_epoch: Epoch, event: EventID) -> Self {
        Self {
            count: NonZeroU32::new(1).expect("1 is non-zero"),
            end_epoch,
            event,
        }
    }

    /// Processes a register status change on the [`Option<PermanentBlobInfo>`] object
    /// representing all permanent blobs.
    pub(super) fn register(
        permanent_total: &mut Option<PermanentBlobInfo>,
        change_info: &BlobStatusChangeInfo,
    ) {
        PermanentBlobInfo::update_optional(permanent_total, change_info)
    }

    /// Processes a certify status change on the [`PermanentBlobInfo`] objects representing all
    /// and the certified permanent blobs.
    ///
    /// Returns whether the update was successful.
    pub(super) fn certify(
        permanent_total: &Option<PermanentBlobInfo>,
        permanent_certified: &mut Option<PermanentBlobInfo>,
        change_info: &BlobStatusChangeInfo,
    ) -> bool {
        let Some(permanent_total) = permanent_total else {
            tracing::error!("attempt to certify a permanent blob when none is tracked");
            return false;
        };

        let registered_end_epoch = permanent_total.end_epoch;
        let certified_end_epoch = change_info.end_epoch;
        if certified_end_epoch > registered_end_epoch {
            tracing::error!(
                registered_end_epoch,
                certified_end_epoch,
                "attempt to certify a permanent blob with later end epoch than any registered blob",
            );
            return false;
        }
        if permanent_total.count.get()
            <= permanent_certified
                .as_ref()
                .map(|p| p.count.get())
                .unwrap_or_default()
        {
            tracing::error!("attempt to certify a permanent blob before corresponding register");
            return false;
        }
        PermanentBlobInfo::update_optional(permanent_certified, change_info);
        true
    }

    /// Processes an extend status change on the [`PermanentBlobInfo`] object representing the
    /// certified permanent blobs.
    pub(super) fn extend(
        permanent_info: &mut Option<PermanentBlobInfo>,
        change_info: &BlobStatusChangeInfo,
    ) {
        let Some(permanent_info) = permanent_info else {
            tracing::error!("attempt to extend a permanent blob when none is tracked");
            return;
        };

        permanent_info.update(change_info, false);
    }

    /// Decrements the count of the given [`PermanentBlobInfo`], removing it if the count reaches
    /// zero.
    ///
    /// This is called when blobs expire at the end of an epoch.
    pub(super) fn decrement(blob_info_inner: &mut Option<PermanentBlobInfo>) {
        match blob_info_inner {
            None => tracing::error!("attempt to delete a permanent blob when none is tracked"),
            Some(PermanentBlobInfo { count, .. }) => {
                if count.get() == 1 {
                    *blob_info_inner = None;
                } else {
                    *count = NonZeroU32::new(count.get() - 1)
                        .expect("we just checked that `count` is at least 2")
                }
            }
        }
    }

    /// Updates `self` with the `change_info`, increasing the count if `increase_count == true`.
    ///
    /// # Panics
    ///
    /// Panics if the change info has `deletable == true`.
    fn update(&mut self, change_info: &BlobStatusChangeInfo, increase_count: bool) {
        assert!(!change_info.deletable);

        if increase_count {
            self.count = self.count.saturating_add(1)
        };
        if change_info.end_epoch > self.end_epoch {
            *self = PermanentBlobInfo {
                count: self.count,
                end_epoch: change_info.end_epoch,
                event: change_info.status_event,
            };
        }
    }

    /// Updates `existing_info` with the change info or creates a new `Self` if the input is `None`.
    ///
    /// # Panics
    ///
    /// Panics if the change info has `deletable == true`.
    fn update_optional(existing_info: &mut Option<Self>, change_info: &BlobStatusChangeInfo) {
        let BlobStatusChangeInfo {
            epoch: _,
            end_epoch: new_end_epoch,
            status_event: new_status_event,
            deletable,
            blob_id: _,
        } = change_info;
        assert!(!deletable);

        match existing_info {
            None => {
                *existing_info = Some(PermanentBlobInfo::new_first(
                    *new_end_epoch,
                    *new_status_event,
                ))
            }
            Some(permanent_blob_info) => permanent_blob_info.update(change_info, true),
        }
    }

    #[cfg(test)]
    pub(super) fn new_fixed_for_testing(count: u32, end_epoch: Epoch, event_seq: u64) -> Self {
        Self {
            count: NonZeroU32::new(count).expect("count must be non-zero"),
            end_epoch,
            event: walrus_sui::test_utils::fixed_event_id_for_testing(event_seq),
        }
    }

    #[cfg(test)]
    pub(super) fn new_for_testing(count: u32, end_epoch: Epoch) -> Self {
        Self {
            count: NonZeroU32::new(count).expect("count must be non-zero"),
            end_epoch,
            event: walrus_sui::test_utils::event_id_for_testing(),
        }
    }
}
