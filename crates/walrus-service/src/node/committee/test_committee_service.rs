// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use std::{
    borrow::Borrow,
    collections::HashMap,
    num::{NonZero, NonZeroU16},
    sync::{Arc, Mutex},
    time::Duration,
};

use rand::{
    SeedableRng,
    rngs::StdRng,
    seq::{IteratorRandom as _, SliceRandom},
};
use tokio::time;
use tower::{ServiceExt as _, util::BoxCloneService};
use walrus_core::{
    DEFAULT_ENCODING,
    Epoch,
    InconsistencyProof,
    PublicKey,
    SliverIndex,
    SliverPairIndex,
    SliverType,
    bft,
    encoding::{
        EncodingConfig,
        EncodingFactory as _,
        GeneralRecoverySymbol,
        Primary,
        PrimaryRecoverySymbol,
        RequiredCount,
        SliverPair,
    },
    inconsistency::PrimaryInconsistencyProof,
    keys::ProtocolKeyPair,
    merkle::MerkleProof,
    messages::InvalidBlobIdMsg,
    metadata::VerifiedBlobMetadataWithId,
};
use walrus_sdk::active_committees::ActiveCommittees;
use walrus_storage_node_client::error::ClientBuildError;
use walrus_sui::types::{Committee, StorageNode as SuiStorageNode};
use walrus_test_utils::{Result as TestResult, async_param_test};

use crate::{
    node::{
        self,
        committee::{
            CommitteeLookupService,
            CommitteeService,
            NodeServiceFactory,
            committee_service::NodeCommitteeService,
            node_service::{NodeServiceError, Request, Response},
        },
        config::CommitteeServiceConfig,
    },
    test_utils,
};

/// Implements [`NodeServiceFactory`] by returning services cloned from a stored map.
///
/// Attempts to make a service for a key not present in the map will result in a service handles
/// all requests by returning [`std::future::pending()`].
#[derive(Default, Debug)]
struct ServiceFactoryMap {
    services: HashMap<PublicKey, BoxCloneService<Request, Response, NodeServiceError>>,
}

impl ServiceFactoryMap {
    /// Constructs a new instance with a single service that is ready to respond.
    fn single_ready<F>(key: PublicKey, func: F) -> Self
    where
        F: FnMut(Request) -> Result<Response, NodeServiceError> + Send + Clone + 'static,
    {
        let mut this = Self::default();
        this.insert_ready(key, func);
        this
    }

    /// Inserts a single service that immediately responds to requests with the provided closure.
    fn insert_ready<F>(&mut self, key: PublicKey, mut func: F)
    where
        F: FnMut(Request) -> Result<Response, NodeServiceError> + Send + Clone + 'static,
    {
        self.services.insert(
            key,
            tower::util::service_fn(move |request| std::future::ready((func)(request)))
                .boxed_clone(),
        );
    }
}

#[async_trait::async_trait]
impl NodeServiceFactory for ServiceFactoryMap {
    type Service = BoxCloneService<Request, Response, NodeServiceError>;

    async fn make_service(
        &mut self,
        info: &SuiStorageNode,
        _encoding_config: &Arc<EncodingConfig>,
    ) -> Result<Self::Service, ClientBuildError> {
        if let Some(service) = self.services.get(&info.public_key) {
            tracing::trace!("returning a configured service");
            return Ok(service.clone());
        }

        tracing::trace!("returning an infinitely pending service");
        Ok(tower::service_fn(|_request| std::future::pending()).boxed_clone())
    }

    fn connect_timeout(&mut self, _timeout: Duration) {}
}

/// Returns true if there are any members that share the same public key.
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

/// A [`CommitteeLookupService`] that responds with the stored [`ActiveCommittees`].
///
/// The stored committees can be modified with a paired [`TestLookupServiceHandle`].
#[derive(Debug)]
struct TestLookupService(Arc<Mutex<ActiveCommittees>>);

#[async_trait::async_trait]
impl CommitteeLookupService for TestLookupService {
    /// Returns the active committees, which are possibly already transitioning.
    async fn get_active_committees(&self) -> Result<ActiveCommittees, anyhow::Error> {
        let latest = self.0.lock().unwrap().clone();
        std::future::ready(Ok(latest)).await
    }
}

/// Allows modifying the committee returned by the paired [`TestLookupServiceHandle`].
#[derive(Debug)]
struct TestLookupServiceHandle(Arc<Mutex<ActiveCommittees>>);

#[cfg(test)]
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
        let previous_committee = (**current.previous_committee().unwrap()).clone();
        let new_active_committee =
            ActiveCommittees::new((**new_committee).clone(), Some(previous_committee));
        *current = new_active_committee;
    }
}

/// Returns a [`TestLookupService`] and a [`TestLookupServiceHandle`] that can be used to modify it,
/// initialized with the provided committee.
fn lookup_service_pair(init: ActiveCommittees) -> (TestLookupService, TestLookupServiceHandle) {
    let current = Arc::new(Mutex::new(init));
    (
        TestLookupService(current.clone()),
        TestLookupServiceHandle(current),
    )
}

#[async_trait::async_trait]
impl CommitteeLookupService for ActiveCommittees {
    async fn get_active_committees(&self) -> Result<ActiveCommittees, anyhow::Error> {
        Ok(self.clone())
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum ShardAssignment {
    /// Each storage node is given a single shard.
    OneEach,
    /// Storage nodes are assigned arbitrary numbers of shards.
    Varied,
}

/// Returns an [`ActiveCommittees`] consisting of a current and previous committee, and a valid
/// committee that can serve as the next committee.
///
/// The number of shards in the committees is 10.
fn valid_committees(
    current_epoch: Epoch,
    assignment: ShardAssignment,
) -> (ActiveCommittees, Committee) {
    let assignments: [&'static [u16]; 3] = if assignment == ShardAssignment::OneEach {
        [&[1; 10]; 3]
    } else {
        [&[2; 5], &[2, 3, 4, 1], &[1, 2, 3, 3, 1]]
    };

    let initial_committee = test_utils::test_committee_with_epoch(assignments[0], current_epoch);
    let next_committee = test_utils::test_committee_with_epoch(assignments[1], current_epoch + 1);
    let previous_committee = current_epoch
        .checked_sub(1)
        .map(|epoch| test_utils::test_committee_with_epoch(assignments[2], epoch));

    (
        ActiveCommittees::new(initial_committee, previous_committee),
        next_committee,
    )
}

macro_rules! assert_timeout {
    ($duration:expr, $future:expr, $msg:expr) => {
        time::timeout($duration, $future).await.expect_err($msg)
    };
    ($future:expr, $msg:expr) => {
        assert_timeout!(Duration::from_mins(1), $future, $msg)
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

    let mut service_map = ServiceFactoryMap::default();
    for index in node_indices_with_metadata {
        let id = committee.members()[*index].public_key.clone();
        let response_metadata = expected_metadata.clone();
        service_map.insert_ready(id, move |_request| {
            Ok(Response::VerifiedMetadata(response_metadata.clone()))
        });
    }

    let committee_service = NodeCommitteeService::builder()
        .randomness(StdRng::seed_from_u64(0))
        .build_with_factory(
            ActiveCommittees::new(
                committee,
                Some(Committee::new(vec![], 0, NonZeroU16::new(10).unwrap()).unwrap()),
            ),
            service_map,
        )
        .await?;

    let returned_metadata = time::timeout(
        Duration::from_mins(1),
        committee_service.get_and_verify_metadata(*expected_metadata.blob_id(), 1),
    )
    .await?;

    assert_eq!(expected_metadata, returned_metadata);

    Ok(())
}

#[tokio::test(start_paused = true)]
async fn new_committee_unavailable_for_reads_until_transition_completes() -> TestResult {
    let expected_metadata = walrus_core::test_utils::verified_blob_metadata();
    let new_epoch: Epoch = 8;

    let (committees, next_committee) = valid_committees(new_epoch - 1, ShardAssignment::Varied);
    debug_assert!(!has_members_in_common(
        committees.current_committee(),
        &next_committee
    ));
    let (committee_lookup, committee_handle) = lookup_service_pair(committees);

    let expected_metadata_clone = expected_metadata.clone();
    let service_map = ServiceFactoryMap::single_ready(
        next_committee.members()[2].public_key.clone(),
        move |_request| Ok(Response::VerifiedMetadata(expected_metadata_clone.clone())),
    );

    let committee_service = NodeCommitteeService::builder()
        .randomness(StdRng::seed_from_u64(1))
        .build_with_factory(committee_lookup, service_map)
        .await?;

    let pending_request =
        committee_service.get_and_verify_metadata(*expected_metadata.blob_id(), 0);
    let mut pending_request = std::pin::pin!(pending_request);

    assert_timeout!(
        &mut pending_request,
        "must timeout since no service in the current committee has metadata"
    );

    committee_handle.begin_transition_to(next_committee);
    committee_service.begin_committee_change(new_epoch).await?;

    assert_timeout!(
        &mut pending_request,
        "must timeout since the new committee is unavailable for reads"
    );

    committee_handle.finish_transition();
    committee_service.end_committee_change(new_epoch)?;

    let returned_metadata = time::timeout(Duration::from_mins(1), &mut pending_request)
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
async fn rejects_non_incremental_epochs(initial_epoch: Epoch, jumped_epoch: Epoch) -> TestResult {
    let (initial_committees, _) = valid_committees(initial_epoch, ShardAssignment::Varied);
    let (committees_skipped_to, _) = valid_committees(jumped_epoch, ShardAssignment::Varied);

    let (committee_lookup, committee_handle) = lookup_service_pair(initial_committees);
    let committee_service = NodeCommitteeService::builder()
        .randomness(StdRng::seed_from_u64(2))
        .build_with_factory(committee_lookup, ServiceFactoryMap::default())
        .await?;

    committee_handle.set_active_committees(committees_skipped_to);

    let _ = committee_service
        .begin_committee_change(initial_epoch + 1)
        .await
        .expect_err("must fail as the next committee is not incremental");

    Ok(())
}

async_param_test! {
    #[tokio::test(start_paused = true)]
    requests_for_metadata_are_dispatched_to_correct_committee -> TestResult: [
        old_metadata_to_old_committee: (1, 1, true),
        very_old_metadata_old_committee: (20, 7, true),
        new_metadata_new_committee: (22, 23, false),
    ]
}
async fn requests_for_metadata_are_dispatched_to_correct_committee(
    initial_epoch: Epoch,
    certified_epoch: Epoch,
    should_read_from_old_committee: bool,
) -> TestResult {
    let expected_metadata = walrus_core::test_utils::verified_blob_metadata();
    let (committees, next_committee) = valid_committees(initial_epoch, ShardAssignment::Varied);
    let current_committee = committees.current_committee();

    debug_assert!(!has_members_in_common(current_committee, &next_committee));

    let expected_metadata_clone = expected_metadata.clone();
    let id = if should_read_from_old_committee {
        &current_committee.members()[3].public_key
    } else {
        &next_committee.members()[1].public_key
    };

    let service_map = ServiceFactoryMap::single_ready(id.clone(), move |_request| {
        Ok(Response::VerifiedMetadata(expected_metadata_clone.clone()))
    });

    let (committee_lookup, handle) = lookup_service_pair(committees);

    let committee_service = NodeCommitteeService::builder()
        .randomness(StdRng::seed_from_u64(3))
        .build_with_factory(committee_lookup, service_map)
        .await?;

    handle.begin_transition_to(next_committee);
    committee_service
        .begin_committee_change(initial_epoch + 1)
        .await?;

    let returned_metadata = time::timeout(
        Duration::from_mins(1),
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
    let blob_id = *expected_metadata.blob_id();

    let (committees, _) = valid_committees(6, ShardAssignment::Varied);
    let local_identity = committees.current_committee().members()[3]
        .public_key
        .clone();

    let service_map = ServiceFactoryMap::single_ready(local_identity.clone(), move |_request| {
        Ok(Response::VerifiedMetadata(expected_metadata.clone()))
    });

    let (committee_lookup, _) = lookup_service_pair(committees);

    let committee_service = NodeCommitteeService::builder()
        .randomness(StdRng::seed_from_u64(4))
        .local_identity(local_identity)
        .build_with_factory(committee_lookup, service_map)
        .await?;

    assert_timeout!(
        committee_service.get_and_verify_metadata(blob_id, 0),
        "must not succeed since only the local node has metadata"
    );

    Ok(())
}

/// For the 0th primary sliver for an arbitrary blob, return a map of a random subset of
/// secondary sliver ID that generated the symbol -> recovery symbols.
fn recovery_symbols_by_shard(
    n_shards: u16,
    rng: &mut StdRng,
) -> TestResult<(
    VerifiedBlobMetadataWithId,
    SliverIndex,
    HashMap<SliverPairIndex, PrimaryRecoverySymbol<MerkleProof>>,
)> {
    let blob = walrus_test_utils::random_data(314);
    let n_shards = NonZero::new(n_shards).unwrap();
    let target_sliver_index = SliverIndex(0);
    let target_sliver_pair_index = target_sliver_index.to_pair_index::<Primary>(n_shards);

    let encoding_config = EncodingConfig::new(n_shards);
    let encoding_config_enum = encoding_config.get_for_type(DEFAULT_ENCODING);
    let (sliver_pairs, metadata) = encoding_config_enum.encode_with_metadata(blob)?;

    let RequiredCount::Exact(n_symbols_for_recovery) =
        encoding_config_enum.n_symbols_for_recovery::<Primary>();
    let recovery_symbols = sliver_pairs
        .iter()
        .map(|pair| {
            let symbol = pair
                .secondary
                .recovery_symbol_for_sliver(target_sliver_pair_index, &encoding_config_enum)
                .unwrap();
            (pair.index(), symbol)
        })
        .choose_multiple(rng, n_symbols_for_recovery);

    Ok((
        metadata,
        target_sliver_index,
        HashMap::from_iter(recovery_symbols),
    ))
}

/// Encodes a random blob for a system with `n_shards` shards and returns the metadata, the
/// sliver pairs, and the encoding config.
fn encoded_blob_for_batch_recovery(
    n_shards: u16,
) -> TestResult<(VerifiedBlobMetadataWithId, Vec<SliverPair>, EncodingConfig)> {
    let blob = walrus_test_utils::random_data(314);
    let n_shards = NonZero::new(n_shards).unwrap();
    let encoding_config = EncodingConfig::new(n_shards);
    let (sliver_pairs, metadata) = encoding_config
        .get_for_type(DEFAULT_ENCODING)
        .encode_with_metadata(blob)?;
    Ok((metadata, sliver_pairs, encoding_config))
}

/// Returns the recovery symbols for the primary `target_indexes` computed from the secondary
/// sliver of `pair`.
fn batch_recovery_symbols_from_pair(
    pair: &SliverPair,
    target_indexes: &[SliverIndex],
    encoding_config: &EncodingConfig,
) -> Vec<GeneralRecoverySymbol> {
    let n_shards = encoding_config.n_shards();
    let config_enum = encoding_config.get_for_type(DEFAULT_ENCODING);
    target_indexes
        .iter()
        .map(|target_index| {
            GeneralRecoverySymbol::from_recovery_symbol(
                pair.secondary
                    .recovery_symbol_for_sliver(
                        target_index.to_pair_index::<Primary>(n_shards),
                        &config_enum,
                    )
                    .expect("valid target index"),
                *target_index,
            )
        })
        .collect()
}

/// How a node in the batched recovery tests responds to batched symbol requests.
#[derive(Debug, Clone, Copy)]
enum BatchNodeBehaviour {
    /// Returns the symbols for all requested targets.
    Full,
    /// Returns the symbols for only the first requested target.
    FirstTargetOnly,
    /// Fails every request.
    AlwaysFails,
    /// Fails the first request and serves all later ones.
    FailsOnce,
}

/// Recovers three primary slivers in one batch from a committee in which two nodes never
/// respond, one node responds only partially, and one node fails its first request.
///
/// With 10 shards, 7 symbols are required per target, so every one of the 7 responding nodes
/// is needed, and the partial and failing nodes force a second round of requests.
#[tokio::test(start_paused = true)]
async fn recovers_slivers_batch_with_partial_and_failing_nodes() -> TestResult {
    let rng = StdRng::seed_from_u64(11);
    let (metadata, sliver_pairs, encoding_config) = encoded_blob_for_batch_recovery(10)?;
    let n_shards = metadata.n_shards();
    let blob_id = *metadata.blob_id();

    let target_pairs = vec![SliverPairIndex(0), SliverPairIndex(3), SliverPairIndex(7)];
    let expected: Vec<_> = target_pairs
        .iter()
        .map(|pair_index| {
            let pair = &sliver_pairs[usize::from(pair_index.get())];
            (*pair_index, walrus_core::Sliver::from(pair.primary.clone()))
        })
        .collect();

    let (committees, _) = valid_committees(1, ShardAssignment::OneEach);
    let committee = committees.current_committee();

    let mut service_map = ServiceFactoryMap::default();
    let mut n_requests_by_node: Vec<Arc<std::sync::atomic::AtomicUsize>> = Vec::new();
    for (position, pair) in sliver_pairs.iter().enumerate() {
        let shard_index = pair.index().to_shard_index(n_shards, &blob_id);
        let node = committee.find_by_shard(shard_index).unwrap();
        let behaviour = match position {
            0 | 1 => BatchNodeBehaviour::AlwaysFails,
            2 => BatchNodeBehaviour::FirstTargetOnly,
            3 => BatchNodeBehaviour::FailsOnce,
            _ => BatchNodeBehaviour::Full,
        };
        let n_requests = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        n_requests_by_node.push(n_requests.clone());

        let pair = pair.clone();
        let encoding_config = encoding_config.clone();
        service_map.insert_ready(node.public_key.clone(), move |request| match request {
            Request::ListVerifiedBatchRecoverySymbols {
                target_indexes,
                target_type,
                ..
            } => {
                assert_eq!(target_type, SliverType::Primary);
                let n_previous = n_requests.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let served: &[SliverIndex] = match behaviour {
                    BatchNodeBehaviour::Full => &target_indexes,
                    BatchNodeBehaviour::FirstTargetOnly => &target_indexes[..1],
                    BatchNodeBehaviour::AlwaysFails => {
                        return Err(NodeServiceError::Other("unavailable".into()));
                    }
                    BatchNodeBehaviour::FailsOnce if n_previous == 0 => {
                        return Err(NodeServiceError::Other("unavailable".into()));
                    }
                    BatchNodeBehaviour::FailsOnce => &target_indexes,
                };
                Ok(Response::VerifiedRecoverySymbols(
                    batch_recovery_symbols_from_pair(&pair, served, &encoding_config),
                ))
            }
            request => panic!("unexpected request: {request:?}"),
        });
    }

    let (committee_lookup, _committee_handle) = lookup_service_pair(committees);
    let committee_service = NodeCommitteeService::builder()
        .randomness(rng)
        .config(CommitteeServiceConfig {
            sliver_request_timeout: Duration::from_secs(1),
            ..Default::default()
        })
        .build_with_factory(committee_lookup, service_map)
        .await?;

    let mut recovered = time::timeout(
        Duration::from_mins(1),
        committee_service.recover_slivers_batch(
            metadata.into(),
            target_pairs.clone(),
            SliverType::Primary,
            1,
        ),
    )
    .await
    .expect("recovery must complete")
    .expect("slivers must be consistent");
    recovered.sort_by_key(|(pair_index, _)| *pair_index);

    assert_eq!(recovered, expected);

    // The partial node and the node that failed once must have been asked again for the
    // targets they did not serve.
    assert!(n_requests_by_node[2].load(std::sync::atomic::Ordering::SeqCst) >= 2);
    assert!(n_requests_by_node[3].load(std::sync::atomic::Ordering::SeqCst) >= 2);

    Ok(())
}

/// Recovers the slivers in several requests per node when the response-size bound only allows a
/// single target per request.
#[tokio::test(start_paused = true)]
async fn recovers_slivers_batch_split_by_response_size() -> TestResult {
    let rng = StdRng::seed_from_u64(12);
    let (metadata, sliver_pairs, encoding_config) = encoded_blob_for_batch_recovery(10)?;
    let n_shards = metadata.n_shards();
    let blob_id = *metadata.blob_id();

    let target_pairs: Vec<_> = (0..4).map(SliverPairIndex).collect();
    let expected: Vec<_> = target_pairs
        .iter()
        .map(|pair_index| {
            let pair = &sliver_pairs[usize::from(pair_index.get())];
            (*pair_index, walrus_core::Sliver::from(pair.primary.clone()))
        })
        .collect();

    let (committees, _) = valid_committees(1, ShardAssignment::OneEach);
    let committee = committees.current_committee();

    let mut service_map = ServiceFactoryMap::default();
    for pair in sliver_pairs.iter() {
        let shard_index = pair.index().to_shard_index(n_shards, &blob_id);
        let node = committee.find_by_shard(shard_index).unwrap();
        let pair = pair.clone();
        let encoding_config = encoding_config.clone();
        service_map.insert_ready(node.public_key.clone(), move |request| match request {
            Request::ListVerifiedBatchRecoverySymbols { target_indexes, .. } => {
                assert_eq!(
                    target_indexes.len(),
                    1,
                    "the response-size bound allows a single target per request"
                );
                Ok(Response::VerifiedRecoverySymbols(
                    batch_recovery_symbols_from_pair(&pair, &target_indexes, &encoding_config),
                ))
            }
            request => panic!("unexpected request: {request:?}"),
        });
    }

    let (committee_lookup, _committee_handle) = lookup_service_pair(committees);
    let committee_service = NodeCommitteeService::builder()
        .randomness(rng)
        .config(CommitteeServiceConfig {
            sliver_request_timeout: Duration::from_secs(1),
            // Smaller than a single symbol with its proof.
            experimental_batched_sliver_recovery_max_response_bytes: 1,
            ..Default::default()
        })
        .build_with_factory(committee_lookup, service_map)
        .await?;

    let mut recovered = time::timeout(
        Duration::from_mins(1),
        committee_service.recover_slivers_batch(
            metadata.into(),
            target_pairs,
            SliverType::Primary,
            1,
        ),
    )
    .await
    .expect("recovery must complete")
    .expect("slivers must be consistent");
    recovered.sort_by_key(|(pair_index, _)| *pair_index);

    assert_eq!(recovered, expected);

    Ok(())
}

#[tokio::test(start_paused = true)]
async fn recovers_slivers_across_epoch_change() -> TestResult {
    let mut rng = StdRng::seed_from_u64(10);

    let (metadata, target_sliver_index, symbols) = recovery_symbols_by_shard(10, &mut rng)?;
    let n_shards = metadata.n_shards();
    let blob_id = *metadata.blob_id();

    let new_epoch: Epoch = 8;
    // Use at most 1 shard per node, to avoid defining request handlers that handle multiple shards
    let (committees, next_committee) = valid_committees(new_epoch - 1, ShardAssignment::OneEach);
    let initial_committee = committees.current_committee();
    debug_assert!(!has_members_in_common(initial_committee, &next_committee));

    let mut service_map = ServiceFactoryMap::default();
    for ((remote_pair_index, symbol), serving_committee) in symbols
        .into_iter()
        .zip([initial_committee, &next_committee].iter().cycle())
    {
        let shard_index = remote_pair_index.to_shard_index(n_shards, &blob_id);
        let node = serving_committee.find_by_shard(shard_index).unwrap();

        debug_assert_eq!(
            node.shard_ids.len(),
            1,
            "test service function requires at most 1 shard per node"
        );

        service_map.insert_ready(node.public_key.clone(), move |request| match request {
            Request::ListVerifiedRecoverySymbols {
                filter,
                target_index,
                ..
            } => {
                let symbol =
                    GeneralRecoverySymbol::from_recovery_symbol(symbol.clone(), target_index);
                assert!(filter.accepts(&symbol));
                Ok(Response::VerifiedRecoverySymbols(vec![symbol]))
            }
            request => panic!("unexpected request: {request:?}"),
        });
    }

    let (committee_lookup, committee_handle) = lookup_service_pair(committees);

    let committee_service = NodeCommitteeService::builder()
        .randomness(rng)
        .config(CommitteeServiceConfig {
            // Reduce timeout duration from 5 mins to play nice with the timeouts used in the test
            sliver_request_timeout: Duration::from_secs(1),
            ..Default::default()
        })
        .build_with_factory(committee_lookup, service_map)
        .await?;

    let pending_request = committee_service.recover_sliver(
        metadata.into(),
        target_sliver_index.into(),
        SliverType::Primary,
        0,
    );
    let mut pending_request = std::pin::pin!(pending_request);

    assert_timeout!(
        &mut pending_request,
        "must timeout since insufficient services in the current committee have symbols"
    );

    committee_handle.begin_transition_to(next_committee);
    committee_service.begin_committee_change(new_epoch).await?;

    assert_timeout!(
        &mut pending_request,
        "must timeout since the new committee is unavailable for reads"
    );

    committee_handle.finish_transition();
    committee_service.end_committee_change(new_epoch)?;

    let _sliver = time::timeout(Duration::from_mins(1), &mut pending_request)
        .await
        .expect("request must succeed since new committee has remaining recovery symbols");

    Ok(())
}

#[tokio::test(start_paused = true)]
async fn restarts_inconsistency_proof_collection_across_epoch_change() -> TestResult {
    let mut rng = StdRng::seed_from_u64(20);
    let blob_id = walrus_core::test_utils::blob_id_from_u64(99);
    let inconsistency_proof = InconsistencyProof::Primary(
        PrimaryInconsistencyProof::<MerkleProof>::new(SliverIndex(0), vec![]),
    );

    let new_epoch: Epoch = 8;
    let (committees, next_committee) = valid_committees(new_epoch - 1, ShardAssignment::OneEach);
    let initial_committee = committees.current_committee();
    debug_assert!(!has_members_in_common(initial_committee, &next_committee));

    let weight_required = bft::min_n_correct(committees.current_committee().n_shards());
    let nodes_required = weight_required.get() as usize; // Since we use 1 shard per node.

    let mut service_map = ServiceFactoryMap::default();

    for committee in [initial_committee.borrow(), &next_committee] {
        for node in committee
            .members()
            .choose_multiple(&mut rng, nodes_required - 1)
        {
            let message = InvalidBlobIdMsg::new(committee.epoch, blob_id);
            // Use an arbitrary key pair since we skip verification in these tests.
            let arbitrary_key_pair = ProtocolKeyPair::generate_with_rng(&mut rng);
            let attestation = node::sign_message(message, arbitrary_key_pair).await?;

            service_map.insert_ready(node.public_key.clone(), move |_request| {
                Ok(attestation.clone().into())
            });
        }
    }

    let (committee_lookup, committee_handle) = lookup_service_pair(committees);

    let committee_service = NodeCommitteeService::builder()
        .randomness(rng)
        .config(CommitteeServiceConfig {
            // Reduce timeout duration to play nicely with the timeouts used in the test
            invalidity_sync_timeout: Duration::from_secs(10),
            ..Default::default()
        })
        .build_with_factory(committee_lookup, service_map)
        .await?;

    let pending_request =
        committee_service.get_invalid_blob_certificate(blob_id, &inconsistency_proof);
    let mut pending_request = std::pin::pin!(pending_request);

    assert_timeout!(
        &mut pending_request,
        "must timeout since insufficient services in the current committee respond"
    );

    committee_handle.begin_transition_to(next_committee);
    committee_service.begin_committee_change(new_epoch).await?;

    assert_timeout!(
        &mut pending_request,
        "must timeout since insufficient services in the current committee respond"
    );

    Ok(())
}

#[tokio::test(start_paused = true)]
async fn collects_inconsistency_proof_despite_epoch_change() -> TestResult {
    let mut rng = StdRng::seed_from_u64(20);
    let blob_id = walrus_core::test_utils::blob_id_from_u64(99);
    let inconsistency_proof = InconsistencyProof::Primary(
        PrimaryInconsistencyProof::<MerkleProof>::new(SliverIndex(0), vec![]),
    );

    let new_epoch: Epoch = 8;
    let (committees, next_committee) = valid_committees(new_epoch - 1, ShardAssignment::OneEach);
    let initial_committee = committees.current_committee();
    debug_assert!(!has_members_in_common(initial_committee, &next_committee));

    let weight_required = bft::min_n_correct(committees.current_committee().n_shards());
    let nodes_required = weight_required.get() as usize; // Since we use 1 shard per node.

    let mut service_map = ServiceFactoryMap::default();
    for (committee, n_responders) in [
        (initial_committee.borrow(), nodes_required - 1),
        (&next_committee, nodes_required),
    ] {
        for node in committee.members().choose_multiple(&mut rng, n_responders) {
            let message = InvalidBlobIdMsg::new(committee.epoch, blob_id);
            // Use an arbitrary key pair since we skip verification in these tests.
            let arbitrary_key_pair = ProtocolKeyPair::generate_with_rng(&mut rng);
            let attestation = node::sign_message(message, arbitrary_key_pair).await?;

            service_map.insert_ready(node.public_key.clone(), move |_request| {
                Ok(attestation.clone().into())
            });
        }
    }

    let (committee_lookup, committee_handle) = lookup_service_pair(committees);

    let committee_service = NodeCommitteeService::builder()
        .randomness(rng)
        .config(CommitteeServiceConfig {
            // Reduce timeout duration to play nicely with the timeouts used in the test
            invalidity_sync_timeout: Duration::from_secs(10),
            ..Default::default()
        })
        .build_with_factory(committee_lookup, service_map)
        .await?;

    let pending_request =
        committee_service.get_invalid_blob_certificate(blob_id, &inconsistency_proof);
    let mut pending_request = std::pin::pin!(pending_request);

    assert_timeout!(
        &mut pending_request,
        "must timeout since insufficient services in the current committee respond"
    );

    committee_handle.begin_transition_to(next_committee);
    committee_service.begin_committee_change(new_epoch).await?;

    let _ = time::timeout(Duration::from_mins(1), &mut pending_request)
        .await
        .expect("request must succeed since new committee has sufficient responses");

    Ok(())
}
