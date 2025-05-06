// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use std::{
    sync::Arc,
    task::{self, Context, Poll},
};

use fastcrypto::hash::Blake2b256;
use futures::{FutureExt as _, TryFutureExt, future::BoxFuture};
use moka::future::Cache;
use prometheus::IntCounter;
use tower::Service;
use walrus_core::{
    BlobId,
    EncodingType,
    Sliver,
    SliverId,
    SliverPairIndex,
    SliverType,
    by_axis::{self, ByAxis},
    encoding::{EncodingConfig, GeneralRecoverySymbol, Primary, RecoverySymbolError, Secondary},
    merkle::MerkleTree,
};
use walrus_utils::metrics::Registry;

use super::thread_pool::{self, BoundedThreadPool};
use crate::utils;

walrus_utils::metrics::define_metric_set! {
    #[namespace = "walrus_recovery_symbol_service"]
    /// Metrics for the recovery symbol service and its cache.
    struct RecoverySymbolCacheMetrics {
        #[help = "The total number of requests made against the `RecoverySymbolService`."]
        requests_total: IntCounter[],

        #[help = "The total number of cache misses in the `RecoverySymbolService`."]
        cache_miss_total: IntCounter[],
    }
}

/// The key into the cache.
///
/// The merkle trees for recovery symbols are cached for each source sliver in a blob.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct CacheKey {
    blob_id: BlobId,
    source_id: SliverId,
}

/// A request to construct a recovery symbol from a sliver.
#[derive(Debug, Clone)]
pub(crate) struct RecoverySymbolRequest {
    /// The blob ID from which the sliver is taken.
    pub blob_id: BlobId,
    /// The source sliver from which the recovery symbol is taken.
    pub source_sliver: Sliver,
    /// The encoding type of the source sliver.
    pub encoding_type: EncodingType,
    /// The index of the sliver on the orthogonal axis which is being recovered.
    pub target_pair_index: SliverPairIndex,
}

/// Service used to create recovery symbols from a sliver.
///
/// Expansion of the sliver and construction of the merkle tree is performed on CPU thread-pool.
///
/// The service also caches the merkle trees used to construct the proofs from a sliver. This
/// allows faster construction of any other recovery symbols from that same source sliver.
///
/// The sliver itself is not cached and must be provided, which ensures that the storage node is
/// still storing the sliver.
///
/// # Cache capacity
///
/// The maximum number of merkle trees that can be stored can be specified with
/// `max_cache_capacity`. The memory usage of the cache can be estimated with
/// `n_shards * max_cache_capacity * 64 B`.
///
/// A storage node stores 2 slivers per blob per owned shard, and so can cache the slivers for
/// `max_cache_capacity / 2` blobs per owned shard.
///
/// So, for a system with with 1000 shards
/// - 1,000 capacity = 64 MB = 500 blobs @ 1 shard | 25 blobs @ 20 | 5 blobs @ 100
/// - 5,000 capacity = 320 MB = 2,500 blobs @ 1 shard | 125 blobs @ 20 | 25 blobs @ 100
/// - 7,500 capacity = 480 MB = 3,750 blobs @ 1 shard | 187 blobs @ 20 | 37 blobs @ 100
/// - 20,000 capacity = 1280 MB = 10,000 blobs @ 1 shard | 500 blobs @ 20 | 100 blobs @ 100
#[derive(Clone, Debug)]
pub(crate) struct RecoverySymbolService {
    cache: Cache<CacheKey, Arc<MerkleTree<Blake2b256>>>,
    thread_pool: BoundedThreadPool,
    encoding_config: Arc<EncodingConfig>,
    metrics: RecoverySymbolCacheMetrics,
}

impl RecoverySymbolService {
    /// Create a new instance of `RecoverySymbolService` with the specified capacity.
    pub(crate) fn new(
        max_cache_capacity: u64,
        encoding_config: Arc<EncodingConfig>,
        thread_pool: BoundedThreadPool,
        registry: &Registry,
    ) -> Self {
        let cache = Cache::builder()
            .name("recovery_symbol_cache")
            .max_capacity(max_cache_capacity)
            .build();
        Self {
            cache,
            thread_pool,
            encoding_config,
            metrics: RecoverySymbolCacheMetrics::new(registry),
        }
    }

    async fn handle_request_and_cache(
        &mut self,
        req: RecoverySymbolRequest,
    ) -> Result<GeneralRecoverySymbol, RecoverySymbolError> {
        let n_shards = self.encoding_config.n_shards();
        let config = self.encoding_config.get_for_type(req.encoding_type);

        let decoding_symbol = by_axis::map!(req.source_sliver.as_ref(), |s| s
            .decoding_symbol_for_sliver(req.target_pair_index, &config))
        .transpose()?;

        let target_sliver_index = match req.source_sliver.r#type() {
            SliverType::Primary => req.target_pair_index.to_sliver_index::<Secondary>(n_shards),
            SliverType::Secondary => req.target_pair_index.to_sliver_index::<Primary>(n_shards),
        };

        let cache_key = CacheKey {
            blob_id: req.blob_id,
            source_id: by_axis::map!(req.source_sliver.as_ref(), |s| s.index),
        };

        self.metrics.requests_total.inc();

        let miss_total = self.metrics.cache_miss_total.clone();
        let encoding_config = self.encoding_config.clone();
        let thread_pool = &mut self.thread_pool;
        let merkle_tree = self
            .cache
            .try_get_with::<_, RecoverySymbolError>(cache_key, async move {
                miss_total.inc();

                thread_pool
                    .call(move || merkle_tree_for_request(req, encoding_config))
                    .map(thread_pool::unwrap_or_resume_panic)
                    .await
            })
            .map_err(Arc::unwrap_or_clone)
            .await?;

        let proof = merkle_tree
            .get_proof(target_sliver_index.as_usize())
            .expect("index is valid as it was valid to get the decoding symbol");

        let recovery_symbol = match decoding_symbol {
            ByAxis::Primary(symbol) => ByAxis::Primary(symbol.with_proof(proof)),
            ByAxis::Secondary(symbol) => ByAxis::Secondary(symbol.with_proof(proof)),
        };
        let recovery_symbol = by_axis::flat_map!(recovery_symbol, |symbol| {
            GeneralRecoverySymbol::from_recovery_symbol(symbol, target_sliver_index)
        });

        Ok(recovery_symbol)
    }
}

impl Service<RecoverySymbolRequest> for RecoverySymbolService {
    type Response = GeneralRecoverySymbol;
    type Error = RecoverySymbolError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let result = task::ready!(<BoundedThreadPool as Service<fn()>>::poll_ready(
            &mut self.thread_pool,
            cx
        ));

        thread_pool::unwrap_or_resume_panic(result);

        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: RecoverySymbolRequest) -> Self::Future {
        let mut this = utils::clone_ready_service(self);

        async move { this.handle_request_and_cache(req).await }.boxed()
    }
}

fn merkle_tree_for_request(
    req: RecoverySymbolRequest,
    encoding_config: Arc<EncodingConfig>,
) -> Result<Arc<MerkleTree<Blake2b256>>, RecoverySymbolError> {
    let encoding_config = encoding_config.get_for_type(req.encoding_type);

    let symbols = by_axis::flat_map!(req.source_sliver.as_ref(), |x| x
        .recovery_symbols(&encoding_config))?;

    Ok(Arc::new(MerkleTree::<Blake2b256>::build(
        symbols.to_symbols(),
    )))
}

#[cfg(test)]
mod tests {
    use futures::stream;
    use rayon::ThreadPoolBuilder as RayonThreadPoolBuilder;
    use thread_pool::{RayonThreadPool, ThreadPoolBuilder, TokioBlockingPool};
    use tokio_stream::StreamExt as _;
    use tower::ServiceExt as _;
    use walrus_core::{
        SliverId,
        SliverIndex,
        encoding::{EncodingConfigTrait, PrimarySliver, SliverPair},
        metadata::VerifiedBlobMetadataWithId,
    };
    use walrus_test_utils::{Result as TestResult, async_param_test};

    use super::*;

    enum ThreadPoolType {
        Rayon,
        Tokio,
    }

    fn symbol_service(
        config: Arc<EncodingConfig>,
        pool_type: ThreadPoolType,
    ) -> RecoverySymbolService {
        let mut builder = ThreadPoolBuilder::default();

        match pool_type {
            ThreadPoolType::Rayon => {
                builder.rayon(RayonThreadPool::new(
                    RayonThreadPoolBuilder::new()
                        .num_threads(1)
                        .build()
                        .expect("thread pool construction must succeed")
                        .into(),
                ));
            }
            ThreadPoolType::Tokio => {
                builder.tokio(TokioBlockingPool::default());
            }
        };

        RecoverySymbolService::new(10, config, builder.build_bounded(), &Registry::default())
    }

    struct TestBlobInfo {
        pairs: Vec<SliverPair>,
        metadata: VerifiedBlobMetadataWithId,
        config: Arc<EncodingConfig>,
    }

    impl TestBlobInfo {
        fn new() -> Self {
            let config = walrus_core::test_utils::encoding_config();

            let blob_data: Vec<_> = (0..255).chain(0..255).collect();
            let (pairs, metadata) = config
                .get_for_type(EncodingType::RS2)
                .encode_with_metadata(&blob_data)
                .expect("encoding succeeds");

            Self {
                pairs,
                metadata,
                config: config.into(),
            }
        }

        fn encoding_type(&self) -> EncodingType {
            self.metadata.metadata().encoding_type()
        }

        fn primary_sliver(&self, index: SliverIndex) -> PrimarySliver {
            self.pairs[usize::from(index.get())].primary.clone()
        }
    }

    fn arbitrary_request() -> RecoverySymbolRequest {
        RecoverySymbolRequest {
            blob_id: walrus_core::test_utils::blob_id_from_u64(7),
            source_sliver: walrus_core::test_utils::sliver(),
            target_pair_index: SliverPairIndex(0),
            encoding_type: EncodingType::RS2,
        }
    }

    #[tokio::test]
    #[should_panic]
    async fn service_must_be_polled_rayon() {
        let mut service = symbol_service(
            walrus_core::test_utils::encoding_config().into(),
            ThreadPoolType::Rayon,
        );
        let _ = service.call(arbitrary_request()).await;
    }

    #[tokio::test]
    #[should_panic]
    async fn service_must_be_polled_tokio() {
        let mut service = symbol_service(
            walrus_core::test_utils::encoding_config().into(),
            ThreadPoolType::Tokio,
        );
        let _ = service.call(arbitrary_request()).await;
    }

    async_param_test! {
        result_is_equivalent_to_recovery_symbol_method -> TestResult: [
            #[cfg(not(msim))]
            use_rayon: (ThreadPoolType::Rayon),
            use_tokio: (ThreadPoolType::Tokio),
        ]
    }
    async fn result_is_equivalent_to_recovery_symbol_method(
        pool_type: ThreadPoolType,
    ) -> TestResult {
        let blob_info = TestBlobInfo::new();
        let n_shards = blob_info.config.n_shards();

        let source_id = SliverId::Primary(SliverIndex(0));
        let target_id = SliverId::Secondary(SliverIndex(0));
        let target_pair_index = target_id.pair_index(n_shards);

        let sliver = blob_info.primary_sliver(source_id.index());

        let expected_recovery_symbol = GeneralRecoverySymbol::from_recovery_symbol(
            sliver.recovery_symbol_for_sliver(
                target_pair_index,
                &blob_info.config.get_for_type(blob_info.encoding_type()),
            )?,
            target_id.index(),
        );

        let mut service = symbol_service(blob_info.config.clone(), pool_type);

        let service = service.ready().now_or_never().unwrap()?;
        let response = service
            .call(RecoverySymbolRequest {
                blob_id: *blob_info.metadata.blob_id(),
                source_sliver: sliver.into(),
                target_pair_index,
                encoding_type: blob_info.encoding_type(),
            })
            .await?;

        assert_eq!(response, expected_recovery_symbol);

        Ok(())
    }

    async_param_test! {
        recovery_symbol_using_cached_proof_is_equivalent_to_recovery_symbol_method -> TestResult: [
            #[cfg(not(msim))]
            use_rayon: (ThreadPoolType::Rayon),
            use_tokio: (ThreadPoolType::Tokio),
        ]
    }
    async fn recovery_symbol_using_cached_proof_is_equivalent_to_recovery_symbol_method(
        pool_type: ThreadPoolType,
    ) -> TestResult {
        let blob_info = TestBlobInfo::new();
        let n_shards = blob_info.config.n_shards();

        let source_id = SliverId::Primary(SliverIndex(0));
        let first_target_id = SliverId::Secondary(SliverIndex(0));
        let target_id = SliverId::Secondary(SliverIndex(1));

        let sliver = blob_info.primary_sliver(source_id.index());

        let expected_recovery_symbol = GeneralRecoverySymbol::from_recovery_symbol(
            sliver.recovery_symbol_for_sliver(
                target_id.pair_index(n_shards),
                &blob_info.config.get_for_type(blob_info.encoding_type()),
            )?,
            target_id.index(),
        );

        let initial_request = RecoverySymbolRequest {
            blob_id: *blob_info.metadata.blob_id(),
            source_sliver: sliver.into(),
            target_pair_index: first_target_id.pair_index(n_shards),
            encoding_type: blob_info.encoding_type(),
        };

        let symbols = symbol_service(blob_info.config.clone(), pool_type)
            .call_all(stream::iter([
                initial_request.clone(),
                RecoverySymbolRequest {
                    target_pair_index: target_id.pair_index(n_shards),
                    ..initial_request
                },
            ]))
            .collect::<Result<Vec<_>, _>>()
            .await?;

        assert_eq!(symbols[1], expected_recovery_symbol);

        Ok(())
    }

    async_param_test! {
        recovery_symbol_for_different_sliver_uses_different_proof -> TestResult: [
            #[cfg(not(msim))]
            use_rayon: (ThreadPoolType::Rayon),
            use_tokio: (ThreadPoolType::Tokio),
        ]
    }
    async fn recovery_symbol_for_different_sliver_uses_different_proof(
        pool_type: ThreadPoolType,
    ) -> TestResult {
        let blob_info = TestBlobInfo::new();
        let n_shards = blob_info.config.n_shards();

        let first_source_id = SliverId::Primary(SliverIndex(0));
        let second_source_id = SliverId::Primary(SliverIndex(2));
        let target_id = SliverId::Secondary(SliverIndex(0));

        let first_sliver = blob_info.primary_sliver(first_source_id.index());
        let second_sliver = blob_info.primary_sliver(second_source_id.index());

        let expected_recovery_symbol = GeneralRecoverySymbol::from_recovery_symbol(
            second_sliver.recovery_symbol_for_sliver(
                target_id.pair_index(n_shards),
                &blob_info.config.get_for_type(blob_info.encoding_type()),
            )?,
            target_id.index(),
        );

        let initial_request = RecoverySymbolRequest {
            blob_id: *blob_info.metadata.blob_id(),
            source_sliver: first_sliver.into(),
            target_pair_index: target_id.pair_index(n_shards),
            encoding_type: blob_info.encoding_type(),
        };

        let symbols = symbol_service(blob_info.config.clone(), pool_type)
            .call_all(stream::iter([
                initial_request.clone(),
                RecoverySymbolRequest {
                    source_sliver: second_sliver.into(),
                    ..initial_request
                },
            ]))
            .collect::<Result<Vec<_>, _>>()
            .await?;

        assert_eq!(symbols[1], expected_recovery_symbol);

        Ok(())
    }
}
