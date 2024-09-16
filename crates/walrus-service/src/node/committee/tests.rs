// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

// #![allow(clippy::mutable_key_type, unused_imports)]
// use std::{
//     collections::{HashMap, HashSet},
//     num::NonZeroU16,
//     sync::Arc,
//     time::Duration,
// };
//
// use futures::FutureExt;
// use mockall::predicate;
// use proptest::{
//     prelude::Strategy,
//     strategy::{Just, ValueTree},
//     test_runner::TestRunner,
// };
// use rand::{
//     rngs::{SmallRng, StdRng},
//     seq::SliceRandom,
//     SeedableRng,
// };
// use tokio::{runtime::Runtime, time};
// use walrus_core::{
//     encoding::EncodingConfig,
//     keys::{NetworkKeyPair, ProtocolKeyPair},
//     metadata::VerifiedBlobMetadataWithId,
//     Epoch,
//     PublicKey,
//     ShardIndex,
// };
// use walrus_sdk::error::{ClientBuildError, NodeError};
// use walrus_sui::types::{Committee, NetworkAddress, StorageNode as SuiStorageNode};
// use walrus_test_utils::{async_param_test, param_test, Result as TestResult};
//
// use super::node_service::{MockNodeService, NodeService, NodeServiceFactory};
// use crate::{
//     node::{
//         committee::{active_committee::ActiveCommittees, committee3::NodeCommitteeServiceInner},
//         config::CommitteeServiceConfig,
//     },
//     test_utils,
// };
//
// fn node_service_with_metadata(metadata: VerifiedBlobMetadataWithId) -> Arc<MockNodeService> {
//     let mut service = MockNodeService::new();
//
//     service
//         .expect_get_verified_metadata()
//         .with(predicate::eq(*metadata.blob_id()))
//         .returning(move |_| Box::pin(std::future::ready(Ok(metadata.clone()))));
//
//     Arc::new(service)
// }
//
// fn pending_node_service() -> Arc<MockNodeService> {
//     let mut service = MockNodeService::new();
//
//     service
//         .expect_get_verified_metadata()
//         .returning(move |_| Box::pin(std::future::pending()));
//
//     Arc::new(service)
// }
//
// fn service_factory_from_list(
//     nodes: &[SuiStorageNode],
//     services: Vec<Arc<MockNodeService>>,
// ) -> Box<dyn NodeServiceFactory<Service = Arc<MockNodeService>>> {
//     debug_assert_eq!(nodes.len(), services.len());
//     let node_ids = nodes.iter().map(|node| node.public_key.clone());
//     let service_map = node_ids.zip(services).collect::<HashMap<_, _>>();
//
//     Box::new(move |node_info: &SuiStorageNode| {
//         let service = service_map
//             .get(&node_info.public_key)
//             .cloned()
//             .expect("services can only be built for committee members");
//         Box::pin(std::future::ready(Ok(service)))
//     })
// }
//
// #[derive(Debug, Default)]
// struct StubServiceFactory(HashMap<PublicKey, Arc<MockNodeService>>);
//
// impl StubServiceFactory {
//     fn with_pending_services<'a, I>(committees: I) -> Self
//     where
//         I: IntoIterator<Item = &'a Committee>,
//     {
//         let mut this = Self::default();
//         for committee in committees {
//             this.extend_for_members(committee, pending_node_service());
//         }
//         this
//     }
//
//     fn extend_for_members(&mut self, committee: &Committee, service: Arc<MockNodeService>) {
//         for member in committee.members() {
//             self.insert(member, service.clone());
//         }
//     }
//
//     fn insert(&mut self, member: &SuiStorageNode, service: Arc<MockNodeService>) {
//         self.0.insert(member.public_key.clone(), service);
//     }
// }
//
// #[async_trait::async_trait]
// impl NodeServiceFactory for StubServiceFactory {
//     type Service = Arc<MockNodeService>;
//
//     async fn make_service(
//         &mut self,
//         info: &SuiStorageNode,
//     ) -> Result<Self::Service, ClientBuildError> {
//         let service = self
//             .0
//             .get(&info.public_key)
//             .cloned()
//             .expect("services can only be built for committee members");
//         Ok(service)
//     }
// }
//
// fn has_members_in_common(committee: &Committee, other: &Committee) -> bool {
//     for member in committee.members() {
//         if other
//             .members()
//             .iter()
//             .any(|other_member| other_member.public_key == member.public_key)
//         {
//             return true;
//         }
//     }
//     false
// }
//
// macro_rules! assert_timeout {
//     ($duration_secs:expr, $future:expr, $msg:expr) => {
//         time::timeout(Duration::from_secs($duration_secs), $future)
//             .await
//             .expect_err($msg)
//     };
//     ($future:expr, $msg:expr) => {
//         assert_timeout!(60, $future, $msg)
//     };
// }
//
// async_param_test! {
//     #[tokio::test(start_paused = true)]
//     metadata_request_succeeds_if_available -> TestResult: [
//          one_available: (test_utils::test_committee(&[1; 10]), &[7]),
//          all_available: (test_utils::test_committee(&[2, 3, 4, 1]), &[0, 1, 2, 3]),
//     ]
// }
// async fn metadata_request_succeeds_if_available(
//     committee: Committee,
//     node_indices_with_metadata: &[usize],
// ) -> TestResult {
//     let rng = StdRng::seed_from_u64(0);
//     let expected_metadata = walrus_core::test_utils::verified_blob_metadata();
//     let encoding_config = EncodingConfig::new(NonZeroU16::new(10).unwrap());
//
//     let mut service_factory = StubServiceFactory::with_pending_services([&committee]);
//     node_indices_with_metadata.iter().for_each(|&i| {
//         service_factory.insert(
//             &committee.members()[i],
//             node_service_with_metadata(expected_metadata.clone()),
//         )
//     });
//
//     let committee_service = NodeCommitteeServiceInner::new(
//         ActiveCommittees::new(committee, None),
//         Box::new(service_factory),
//         CommitteeServiceConfig::default(),
//         None,
//         Arc::new(encoding_config),
//         rng,
//     )
//     .await?;
//
//     let returned_metadata = committee_service
//         .get_and_verify_metadata(expected_metadata.blob_id(), 0)
//         .await;
//
//     assert_eq!(expected_metadata, returned_metadata);
//
//     Ok(())
// }
//
// #[tokio::test(start_paused = true)]
// async fn new_committee_unavailable_for_reads_until_transition_completes() -> TestResult {
//     let rng = StdRng::seed_from_u64(1);
//     let encoding_config = EncodingConfig::new(NonZeroU16::new(10).unwrap());
//     let expected_metadata = walrus_core::test_utils::verified_blob_metadata();
//
//     let prior_committee = test_utils::test_committee_with_epoch(&[2; 5], 6);
//     let initial_committee = test_utils::test_committee_with_epoch(&[2; 5], 7);
//     let next_committee = test_utils::test_committee_with_epoch(&[2, 3, 4, 1], 8);
//     debug_assert!(!has_members_in_common(&initial_committee, &next_committee));
//
//     let mut service_map =
//         StubServiceFactory::with_pending_services([&initial_committee, &next_committee]);
//     service_map.insert(
//         &next_committee.members()[2],
//         node_service_with_metadata(expected_metadata.clone()),
//     );
//
//     let committee_service = NodeCommitteeServiceInner::new(
//         ActiveCommittees::new(initial_committee, Some(prior_committee)),
//         Box::new(service_map),
//         CommitteeServiceConfig::default(),
//         None,
//         Arc::new(encoding_config),
//         rng,
//     )
//     .await?;
//
//     let mut pending_request =
//         committee_service.get_and_verify_metadata(expected_metadata.blob_id(), 0);
//     let mut pending_request = std::pin::pin!(pending_request);
//
//     assert_timeout!(
//         &mut pending_request,
//         "must timeout since no service in the current committee has metadata"
//     );
//
//     committee_service
//         .begin_committee_transition(next_committee)
//         .await;
//
//     assert_timeout!(
//         &mut pending_request,
//         "must timeout since the new committee is unavailable for reads"
//     );
//
//     committee_service.end_committee_transition();
//
//     let returned_metadata = time::timeout(Duration::from_secs(60), &mut pending_request)
//         .await
//         .expect("request must succeed since new committee has metadata");
//
//     assert_eq!(returned_metadata, expected_metadata);
//     Ok(())
// }
//
// async_param_test! {
//     #[tokio::test(start_paused = true)]
//     panics_for_non_incremental_epochs -> (): [
//          zero_to_two: (0, 2),
//          nth_to_other: (71, 99),
//     ]
// }
// #[should_panic(expected = "next committee must be for the next epoch")]
// async fn panics_for_non_incremental_epochs(initial_epoch: Epoch, next_epoch: Epoch) {
//     let rng = StdRng::seed_from_u64(2);
//     let encoding_config = EncodingConfig::new(NonZeroU16::new(10).unwrap());
//
//     let prior_committee = (initial_epoch != 0)
//         .then(|| test_utils::test_committee_with_epoch(&[2; 5], initial_epoch - 1));
//     let initial_committee = test_utils::test_committee_with_epoch(&[2; 5], initial_epoch);
//     let next_committee = test_utils::test_committee_with_epoch(&[2, 3, 4, 1], next_epoch);
//     debug_assert!(!has_members_in_common(&initial_committee, &next_committee));
//
//     let service_map =
//         StubServiceFactory::with_pending_services([&initial_committee, &next_committee]);
//     let committee_service = NodeCommitteeServiceInner::new(
//         ActiveCommittees::new(initial_committee, prior_committee),
//         Box::new(service_map),
//         CommitteeServiceConfig::default(),
//         None,
//         Arc::new(encoding_config),
//         rng,
//     )
//     .await
//     .unwrap();
//
//     committee_service
//         .begin_committee_transition(next_committee)
//         .await;
// }
//
// async_param_test! {
//     #[tokio::test(start_paused = true)]
//     requests_for_metadata_are_dipsatched_to_correct_committee -> TestResult: [
//          old_metadata_to_old_committee: (0, 0, true),
//          very_old_metadata_old_committee: (20, 7, true),
//          new_metadata_new_committee: (22, 23, false),
//     ]
// }
// async fn requests_for_metadata_are_dipsatched_to_correct_committee(
//     initial_epoch: Epoch,
//     certified_epoch: Epoch,
//     should_read_from_old_committee: bool,
// ) -> TestResult {
//     let rng = StdRng::seed_from_u64(3);
//     let encoding_config = EncodingConfig::new(NonZeroU16::new(10).unwrap());
//     let expected_metadata = walrus_core::test_utils::verified_blob_metadata();
//
//     let prior_committee = (initial_epoch != 0)
//         .then(|| test_utils::test_committee_with_epoch(&[2; 5], initial_epoch - 1));
//     let initial_committee = test_utils::test_committee_with_epoch(&[2; 5], initial_epoch);
//     let next_committee = test_utils::test_committee_with_epoch(&[2, 3, 4, 1], initial_epoch + 1);
//     debug_assert!(!has_members_in_common(&initial_committee, &next_committee));
//
//     let mut service_map =
//         StubServiceFactory::with_pending_services([&initial_committee, &next_committee]);
//     let member_with_metadata = if should_read_from_old_committee {
//         &initial_committee.members()[2]
//     } else {
//         &next_committee.members()[2]
//     };
//     service_map.insert(
//         member_with_metadata,
//         node_service_with_metadata(expected_metadata.clone()),
//     );
//
//     let committee_service = NodeCommitteeServiceInner::new(
//         ActiveCommittees::new(initial_committee, prior_committee),
//         Box::new(service_map),
//         CommitteeServiceConfig::default(),
//         None,
//         Arc::new(encoding_config),
//         rng,
//     )
//     .await?;
//
//     committee_service
//         .begin_committee_transition(next_committee)
//         .await;
//
//     let returned_metadata = time::timeout(
//         Duration::from_secs(60),
//         committee_service.get_and_verify_metadata(expected_metadata.blob_id(), certified_epoch),
//     )
//     .await
//     .expect("request must succeed since new committee has metadata");
//
//     assert_eq!(returned_metadata, expected_metadata);
//     Ok(())
// }
//
// #[tokio::test(start_paused = true)]
// async fn metadata_requests_do_not_query_self() -> TestResult {
//     let rng = StdRng::seed_from_u64(2);
//     let encoding_config = EncodingConfig::new(NonZeroU16::new(10).unwrap());
//     let expected_metadata = walrus_core::test_utils::verified_blob_metadata();
//
//     let prior_committee = test_utils::test_committee_with_epoch(&[2; 5], 6);
//     let committee = test_utils::test_committee_with_epoch(&[2; 5], 7);
//
//     let local_node_info = &committee.members()[3];
//     let local_identity = local_node_info.public_key.clone();
//     let mut service_map = StubServiceFactory::with_pending_services([&committee]);
//     service_map.insert(
//         local_node_info,
//         node_service_with_metadata(expected_metadata.clone()),
//     );
//
//     let committee_service = NodeCommitteeServiceInner::new(
//         ActiveCommittees::new(committee, Some(prior_committee)),
//         Box::new(service_map),
//         CommitteeServiceConfig::default(),
//         Some(local_identity),
//         Arc::new(encoding_config),
//         rng,
//     )
//     .await?;
//
//     assert_timeout!(
//         committee_service.get_and_verify_metadata(expected_metadata.blob_id(), 0),
//         "must not succeed since only the local node has metadata"
//     );
//
//     Ok(())
// }
