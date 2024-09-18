// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use rand::{rngs::StdRng, SeedableRng as _};
use tokio::time;
use tower::{util::BoxCloneService, ServiceExt as _};
use walrus_core::{encoding::EncodingConfig, Epoch, PublicKey};
use walrus_sdk::error::ClientBuildError;
use walrus_sui::types::{Committee, StorageNode as SuiStorageNode};
use walrus_test_utils::{async_param_test, Result as TestResult};

use crate::{
    node::committee::{
        active_committees::BeginCommitteeChangeError,
        committee_service::{CommitteeTransitionError, NodeCommitteeService},
        node_service::{NodeServiceError, Request, Response},
        ActiveCommittees,
        CommitteeLookupService,
        CommitteeService,
        NodeServiceFactory,
    },
    test_utils,
};

#[derive(Default)]
struct StubServiceFactory {
    services: HashMap<PublicKey, BoxCloneService<Request, Response, NodeServiceError>>,
}

#[async_trait::async_trait]
impl NodeServiceFactory for StubServiceFactory {
    type Service = BoxCloneService<Request, Response, NodeServiceError>;

    async fn make_service(
        &mut self,
        info: &SuiStorageNode,
        _encoding_config: &Arc<EncodingConfig>,
    ) -> Result<Self::Service, ClientBuildError> {
        if let Some(service) = self.services.get(&info.public_key) {
            return Ok(service.clone());
        }

        Ok(tower::service_fn(|_request| std::future::pending()).boxed_clone())
    }
}

#[async_trait::async_trait]
impl CommitteeLookupService for ActiveCommittees {
    async fn get_active_committees(&self) -> Result<ActiveCommittees, anyhow::Error> {
        Ok(self.clone())
    }
}

fn has_members_in_common(committee: &Committee, other: &Committee) -> bool {
    for member in committee.members() {
        if other
            .members()
            .iter()
            .any(|other_member| other_member.public_key == member.public_key)
        {
            return true;
        }
    }
    false
}

#[derive(Debug)]
struct TestLookupService(Arc<Mutex<ActiveCommittees>>);

impl TestLookupService {
    fn new_with_handle(init: ActiveCommittees) -> (Self, TestLookupServiceHandle) {
        let current = Arc::new(Mutex::new(init));
        (Self(current.clone()), TestLookupServiceHandle(current))
    }
}

#[derive(Debug)]
struct TestLookupServiceHandle(Arc<Mutex<ActiveCommittees>>);

impl TestLookupServiceHandle {
    fn set_active_committees(&self, committees: ActiveCommittees) {
        let mut current = self.0.lock().unwrap();
        *current = committees;
    }

    fn begin_transition_to(&self, next: Committee) {
        let mut current = self.0.lock().unwrap();
        let new_active_committee =
            ActiveCommittees::new_transitioning(next, (**current.current_committee()).clone());
        *current = new_active_committee;
    }

    fn finish_transition(&self) {
        let mut current = self.0.lock().unwrap();
        let new_committee = current.current_committee();
        let prior_committee = (**current.prior_committee().unwrap()).clone();
        let new_active_committee =
            ActiveCommittees::new((**new_committee).clone(), Some(prior_committee));
        *current = new_active_committee;
    }
}

/// Service used to query the current, prior, and upcoming committees.
#[async_trait::async_trait]
impl CommitteeLookupService for TestLookupService {
    /// Returns the active committees, which are possibly already transitioning.
    async fn get_active_committees(&self) -> Result<ActiveCommittees, anyhow::Error> {
        let latest = self.0.lock().unwrap().clone();
        std::future::ready(Ok(latest)).await
    }
}

macro_rules! assert_timeout {
    ($duration_secs:expr, $future:expr, $msg:expr) => {
        time::timeout(Duration::from_secs($duration_secs), $future)
            .await
            .expect_err($msg)
    };
    ($future:expr, $msg:expr) => {
        assert_timeout!(60, $future, $msg)
    };
}

async_param_test! {
    #[tokio::test(start_paused = true)]
    metadata_request_succeeds_if_available -> TestResult: [
        one_available: (test_utils::test_committee(&[1; 10]), &[7]),
        all_available: (test_utils::test_committee(&[2, 3, 4, 1]), &[0, 1, 2, 3]),
    ]
}
async fn metadata_request_succeeds_if_available(
    committee: Committee,
    node_indices_with_metadata: &[usize],
) -> TestResult {
    let expected_metadata = walrus_core::test_utils::verified_blob_metadata();

    let mut service_factory = StubServiceFactory::default();
    service_factory
        .services
        .extend(node_indices_with_metadata.iter().map(|index| {
            let id = committee.members()[*index].public_key.clone();
            let response_metadata = expected_metadata.clone();
            let service = tower::util::service_fn(move |_request| {
                std::future::ready(Ok(Response::VerifiedMetadata(response_metadata.clone())))
            });

            (id, service.boxed_clone())
        }));

    let committee_service = NodeCommitteeService::builder()
        .node_service_factory(service_factory)
        .randomness(StdRng::seed_from_u64(0))
        .build(ActiveCommittees::new(committee, None))
        .await?;

    let returned_metadata = committee_service
        .get_and_verify_metadata(*expected_metadata.blob_id(), 0)
        .await;

    assert_eq!(expected_metadata, returned_metadata);

    Ok(())
}

#[tokio::test(start_paused = true)]
async fn new_committee_unavailable_for_reads_until_transition_completes() -> TestResult {
    let expected_metadata = walrus_core::test_utils::verified_blob_metadata();

    const NEW_EPOCH: Epoch = 8;
    let prior_committee = test_utils::test_committee_with_epoch(&[2; 5], NEW_EPOCH - 2);
    let initial_committee = test_utils::test_committee_with_epoch(&[2; 5], NEW_EPOCH - 1);
    let next_committee = test_utils::test_committee_with_epoch(&[2, 3, 4, 1], NEW_EPOCH);
    debug_assert!(!has_members_in_common(&initial_committee, &next_committee));

    let mut service_factory = StubServiceFactory::default();
    let response_metadata = expected_metadata.clone();
    service_factory.services.insert(
        next_committee.members()[2].public_key.clone(),
        tower::util::service_fn(move |_request| {
            std::future::ready(Ok(Response::VerifiedMetadata(response_metadata.clone())))
        })
        .boxed_clone(),
    );

    let (committee_lookup, handle) = TestLookupService::new_with_handle(ActiveCommittees::new(
        initial_committee,
        Some(prior_committee),
    ));

    let committee_service = NodeCommitteeService::builder()
        .node_service_factory(service_factory)
        .randomness(StdRng::seed_from_u64(1))
        .build(committee_lookup)
        .await?;

    let mut pending_request =
        committee_service.get_and_verify_metadata(*expected_metadata.blob_id(), 0);
    let mut pending_request = std::pin::pin!(pending_request);

    assert_timeout!(
        &mut pending_request,
        "must timeout since no service in the current committee has metadata"
    );

    handle.begin_transition_to(next_committee);
    committee_service.begin_committee_change().await?;

    assert_timeout!(
        &mut pending_request,
        "must timeout since the new committee is unavailable for reads"
    );

    handle.finish_transition();
    committee_service.end_committee_change(NEW_EPOCH)?;

    let returned_metadata = time::timeout(Duration::from_secs(60), &mut pending_request)
        .await
        .expect("request must succeed since new committee has metadata");

    assert_eq!(returned_metadata, expected_metadata);
    Ok(())
}

async_param_test! {
    #[tokio::test(start_paused = true)]
    rejects_non_incremental_epochs -> TestResult: [
        zero_to_two: (0, 2),
        nth_to_other: (71, 99),
    ]
}
async fn rejects_non_incremental_epochs(initial_epoch: Epoch, next_epoch: Epoch) -> TestResult {
    let prior_committee = (initial_epoch != 0)
        .then(|| test_utils::test_committee_with_epoch(&[2; 5], initial_epoch - 1));
    let initial_committee = test_utils::test_committee_with_epoch(&[2; 5], initial_epoch);

    let committee_skipped_to = test_utils::test_committee_with_epoch(&[2, 3, 4, 1], next_epoch);
    let committee_skipped_to_prior =
        test_utils::test_committee_with_epoch(&[2, 3, 4, 1], next_epoch - 1);

    let (committee_lookup, handle) = TestLookupService::new_with_handle(ActiveCommittees::new(
        initial_committee,
        prior_committee,
    ));
    let committee_service = NodeCommitteeService::builder()
        .node_service_factory(StubServiceFactory::default())
        .randomness(StdRng::seed_from_u64(2))
        .build(committee_lookup)
        .await?;

    handle.set_active_committees(ActiveCommittees::new(
        committee_skipped_to,
        Some(committee_skipped_to_prior),
    ));

    let err = committee_service
        .begin_committee_change()
        .await
        .expect_err("must fail as the upcoming committee is not incremental");

    assert!(matches!(
        dbg!(err),
        CommitteeTransitionError::OutOfSync(BeginCommitteeChangeError::InvalidEpoch { .. })
    ));

    Ok(())
}

async_param_test! {
    #[tokio::test(start_paused = true)]
    requests_for_metadata_are_dipsatched_to_correct_committee -> TestResult: [
        old_metadata_to_old_committee: (0, 0, true),
        very_old_metadata_old_committee: (20, 7, true),
        new_metadata_new_committee: (22, 23, false),
    ]
}
async fn requests_for_metadata_are_dipsatched_to_correct_committee(
    initial_epoch: Epoch,
    certified_epoch: Epoch,
    should_read_from_old_committee: bool,
) -> TestResult {
    let expected_metadata = walrus_core::test_utils::verified_blob_metadata();

    let prior_committee = (initial_epoch != 0)
        .then(|| test_utils::test_committee_with_epoch(&[2; 5], initial_epoch - 1));
    let initial_committee = test_utils::test_committee_with_epoch(&[2; 5], initial_epoch);
    let next_committee = test_utils::test_committee_with_epoch(&[2, 3, 4, 1], initial_epoch + 1);
    debug_assert!(!has_members_in_common(&initial_committee, &next_committee));

    let mut service_factory = StubServiceFactory::default();
    let response_metadata = expected_metadata.clone();
    let id_with_metadata = if should_read_from_old_committee {
        &initial_committee.members()[1].public_key
    } else {
        &next_committee.members()[3].public_key
    };

    service_factory.services.insert(
        id_with_metadata.clone(),
        tower::util::service_fn(move |_request| {
            std::future::ready(Ok(Response::VerifiedMetadata(response_metadata.clone())))
        })
        .boxed_clone(),
    );

    let (committee_lookup, handle) = TestLookupService::new_with_handle(ActiveCommittees::new(
        initial_committee,
        prior_committee,
    ));

    let committee_service = NodeCommitteeService::builder()
        .node_service_factory(service_factory)
        .randomness(StdRng::seed_from_u64(3))
        .build(committee_lookup)
        .await?;

    handle.begin_transition_to(next_committee);
    committee_service.begin_committee_change().await?;

    let returned_metadata = time::timeout(
        Duration::from_secs(60),
        committee_service.get_and_verify_metadata(*expected_metadata.blob_id(), certified_epoch),
    )
    .await
    .expect("request must succeed since some committee has metadata");

    assert_eq!(returned_metadata, expected_metadata);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn metadata_requests_do_not_query_self() -> TestResult {
    let expected_metadata = walrus_core::test_utils::verified_blob_metadata();

    let prior_committee = test_utils::test_committee_with_epoch(&[2; 5], 6);
    let committee = test_utils::test_committee_with_epoch(&[2; 5], 7);

    let local_node_info = &committee.members()[3];
    let local_identity = local_node_info.public_key.clone();
    let response_metadata = expected_metadata.clone();
    let mut service_factory = StubServiceFactory::default();
    service_factory.services.insert(
        local_identity.clone(),
        tower::util::service_fn(move |_request| {
            std::future::ready(Ok(Response::VerifiedMetadata(response_metadata.clone())))
        })
        .boxed_clone(),
    );

    let (committee_lookup, _) =
        TestLookupService::new_with_handle(ActiveCommittees::new(committee, Some(prior_committee)));

    let committee_service = NodeCommitteeService::builder()
        .node_service_factory(service_factory)
        .randomness(StdRng::seed_from_u64(4))
        .local_identity(local_identity)
        .build(committee_lookup)
        .await?;

    assert_timeout!(
        committee_service.get_and_verify_metadata(*expected_metadata.blob_id(), 0),
        "must not succeed since only the local node has metadata"
    );

    Ok(())
}
