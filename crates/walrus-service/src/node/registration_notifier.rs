// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use tokio::sync::{Notify, futures::Notified};
use walrus_core::BlobId;

/// RegistrationNotifier notifies waiters when a blob registration is observed.
#[derive(Clone, Debug, Default)]
pub struct RegistrationNotifier {
    registered_blobs: Arc<Mutex<HashMap<BlobId, RegistrationNotify>>>,
}

impl RegistrationNotifier {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn acquire(&self, blob_id: &BlobId) -> RegistrationNotify {
        let mut registered_blobs = self
            .registered_blobs
            .lock()
            .expect("mutex should not be poisoned");

        registered_blobs
            .entry(*blob_id)
            .or_insert_with(|| RegistrationNotify::new(*blob_id, Arc::new(self.clone())))
            .clone()
    }

    pub fn notify_registered(&self, blob_id: &BlobId) {
        let registered_blobs = self
            .registered_blobs
            .lock()
            .expect("mutex should not be poisoned");
        if let Some(notify) = registered_blobs.get(blob_id) {
            tracing::debug!(%blob_id, "notify registration");
            notify.notify_waiters();
        }
    }

    fn cleanup_registration_notify(&self, blob_id: &BlobId) {
        let mut registered_blobs = self
            .registered_blobs
            .lock()
            .expect("mutex should not be poisoned");

        if let Some(notify) = registered_blobs.get(blob_id) {
            let current_count = *notify
                .ref_count
                .lock()
                .expect("mutex should not be poisoned");

            if current_count == 1 {
                registered_blobs.remove(blob_id);
            }
        }
    }
}

/// RegistrationNotify is a wrapper around Notify to signal a single blob's registration.
#[derive(Debug)]
pub struct RegistrationNotify {
    blob_id: BlobId,
    notify: Arc<Notify>,
    ref_count: Arc<Mutex<usize>>,
    node_wide_registration_notifier: Arc<RegistrationNotifier>,
}

impl RegistrationNotify {
    pub fn new(blob_id: BlobId, notifier: Arc<RegistrationNotifier>) -> Self {
        Self {
            blob_id,
            notify: Arc::new(Notify::new()),
            ref_count: Arc::new(Mutex::new(1)),
            node_wide_registration_notifier: notifier,
        }
    }

    pub fn notified(&self) -> Notified<'_> {
        self.notify.notified()
    }

    fn notify_waiters(&self) {
        self.notify.notify_waiters();
    }
}

impl Clone for RegistrationNotify {
    fn clone(&self) -> Self {
        let mut ref_count = self.ref_count.lock().expect("mutex should not be poisoned");
        *ref_count = ref_count.checked_add(1).unwrap_or(0);
        Self {
            blob_id: self.blob_id,
            notify: self.notify.clone(),
            ref_count: self.ref_count.clone(),
            node_wide_registration_notifier: self.node_wide_registration_notifier.clone(),
        }
    }
}

impl Drop for RegistrationNotify {
    fn drop(&mut self) {
        let new_ref_count = {
            let mut ref_count = self.ref_count.lock().expect("mutex should not be poisoned");
            if *ref_count == 0 {
                tracing::error!(
                    %self.blob_id,
                    "RegistrationNotifier drop: ref count is 0",
                );
            }
            *ref_count = ref_count.checked_sub(1).unwrap_or(0);
            *ref_count
        };

        if new_ref_count == 1 {
            self.node_wide_registration_notifier
                .cleanup_registration_notify(&self.blob_id);
        }
    }
}
