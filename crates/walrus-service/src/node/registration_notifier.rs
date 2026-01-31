// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use walrus_core::BlobId;

use super::ref_counted_notify_map::{RefCountedNotify, RefCountedNotifyMap};

/// RegistrationNotifier notifies waiters when a blob registration is observed.
#[derive(Clone, Debug, Default)]
pub struct RegistrationNotifier {
    registered_blobs: RefCountedNotifyMap<BlobId>,
}

impl RegistrationNotifier {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn acquire(&self, blob_id: &BlobId) -> RegistrationNotify {
        self.registered_blobs.acquire(blob_id)
    }

    pub fn notify_registered(&self, blob_id: &BlobId) {
        if self.registered_blobs.notify(blob_id) {
            tracing::debug!(%blob_id, "notify registration");
        }
    }
}

/// RegistrationNotify is a wrapper around Notify to signal a single blob's registration.
pub type RegistrationNotify = RefCountedNotify<BlobId>;
