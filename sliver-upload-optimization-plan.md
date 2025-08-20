# Upload Latency Optimization: Multi-Put and Cached Upload

## Executive Summary
This branch implements two major optimizations to reduce blob upload latency in Walrus: **Multi-Put batching** and **Cached blob upload**. These changes enable parallel processing of multiple blobs and eliminate redundant work during the upload pipeline, significantly improving throughput and reducing end-to-end latency.

## 1. Multi-Put Batching

### Problem
Previously, blob uploads were processed sequentially - each blob required separate HTTP requests to storage nodes for metadata and sliver storage, creating significant network overhead and limiting throughput.

### Solution
Implemented a batched multi-put API that allows sending multiple blobs to storage nodes in a single HTTP request:

- **API Changes**: Added `MultiPutRequest`/`MultiPutResponse` structures in `walrus-storage-node-client` to batch multiple blob bundles
- **Client Implementation**: New `reserve_and_store_encoded_blobs_parallel` method in SDK client that:
  - Groups sliver pairs by destination storage node
  - Constructs `MultiPutBundle` for each blob containing metadata and relevant slivers
  - Sends all blobs destined for a node in a single batched request
  - Processes responses in parallel across all nodes

### Benefits
- **Reduced Network Overhead**: One HTTP request per node instead of one per blob
- **Improved Throughput**: Multiple blobs processed simultaneously
- **Better Resource Utilization**: Amortizes connection setup and TLS handshake costs

## 2. Cached Blob Upload

### Problem
The blob upload process requires coordination between two asynchronous events:
1. Blob data arrival (slivers and metadata from client)
2. Blob registration event (from blockchain)

Previously, these were processed sequentially, causing unnecessary delays when data arrived before registration.

### Solution
Implemented `CachedBlobManager` that decouples data arrival from registration:

- **State Management**: Tracks blob state through lifecycle:
  - `DataOnly`: Data arrived, waiting for registration
  - `RegisteredOnly`: Registration arrived, waiting for data
  - `Processing`: Both available, computing storage confirmation
  - `Completed`: Confirmation cached for deduplication

- **Parallel Processing**: When both data and registration are available, immediately triggers background processing
- **Deduplication**: Caches successful confirmations to handle duplicate requests without reprocessing

### Key Components
- **Multi-Put Handler** (`cache_blob_slivers`): Accepts batched blob data, stores in cache, returns futures that resolve when registration arrives
- **Registration Handler**: Processes blockchain events, triggers processing for cached data
- **Background Processing**: Asynchronously stores metadata, writes slivers, and computes confirmations

### Benefits
- **Reduced Latency**: Processing begins immediately when both conditions are met
- **Improved Parallelism**: Multiple blobs process concurrently
- **Server-Side Deduplication**: Cached confirmations eliminate redundant work

## 3. Supporting Changes

### BlobPersistence Simplification
- Replaced `BlobPersistenceType` with simpler `BlobPersistence` enum in `MultiPutBundle`
- New type only indicates Permanent/Deletable without requiring object_id upfront
- Simplifies API for clients setting up multi-put bundles

### Implementation Details
- **Graceful Shutdown**: CancellationToken ensures clean termination of background tasks
- **Memory Management**: Automatic cache cleanup after configurable duration
- **Error Handling**: Failed processing removes entries from cache to allow retries

## Performance Impact

The combined optimizations enable:
- **Parallel blob upload**: Multiple blobs sent to all storage nodes simultaneously
- **Overlapped processing**: Data storage can begin before blockchain registration completes
- **Batched network operations**: Significantly reduced HTTP request count
- **Zero redundant work**: Deduplication via cached confirmations

These improvements are particularly effective for workloads involving multiple blob uploads, where the batching and caching effects compound to deliver substantial latency reductions.

## Code Locations
- Multi-Put API: `/crates/walrus-storage-node-client/src/api.rs`
- Parallel Upload: `/crates/walrus-sdk/src/client.rs::reserve_and_store_encoded_blobs_parallel`
- Cached Manager: `/crates/walrus-service/src/node/cached_blob_manager.rs`
- Node Integration: `/crates/walrus-service/src/node.rs::cache_blob_slivers`

---

## Legacy Implementation Plan

### Phase 1: Add RocksDB Cache for Pre-registered Slivers
**Files:** `crates/walrus-service/src/node/storage.rs`

#### 1.1 Add new RocksDB column family
- Column family name: `pre_registered_blobs`

#### 1.2 Define cache structure
```rust
// Key: BlobId
// Value: CachedBlobSlivers
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachedBlobSlivers {
    /// All sliver pairs this node should store for this blob
    pub sliver_pairs: Vec<SliverPair>,
    /// Metadata for the blob (single copy)
    pub metadata: Option<VerifiedBlobMetadataWithId>,
    /// When this was cached
    pub cached_at: SystemTime,
}
```

#### 1.3 Cache management methods
- `cache_blob_slivers_and_metadata(blob_id, pairs, metadata)` - Store blob data in cache
- `get_cached_blob(blob_id)` - Retrieve cached data for a blob
- `remove_cached_blob(blob_id)` - Remove after processing
- `cleanup_expired_cache()` - Background task to remove expired entries

**Test:** Unit tests for cache operations with TTL verification

---

### Phase 2: Define Multi-put API Request/Response Structures
**Files:**
- `crates/walrus-storage-node-client/src/api.rs`
- `crates/walrus-sdk/src/client/communication/node.rs`

```rust
// In api.rs
#[derive(Serialize, Deserialize)]
pub struct MultiPutRequest {
    /// Multiple blobs with their slivers for this node
    pub bundles: Vec<MultiPutBundle>,
    pub target_epoch: Epoch,
}

#[derive(Serialize, Deserialize)]
pub struct MultiPutBundle {
    pub blob_id: BlobId,
    /// Metadata with ID (required for verification and certificate generation)
    pub metadata: VerifiedBlobMetadataWithId,
    /// All sliver pairs this node should store for this blob
    pub sliver_pairs: Vec<SliverPair>,
    /// Blob persistence type for certificate generation
    pub blob_persistence_type: BlobPersistenceType,
}

#[derive(Serialize, Deserialize)]
pub struct MultiPutResponse {
    /// Results for each bundle in the request (same order)
    /// Each result contains the signed confirmation and the weight (number of shards)
    pub results: Vec<NodeResult<SignedStorageConfirmation, StoreError>>,
}

// In client/communication/node.rs
impl NodeWriteCommunication<'_> {
    /// Stores multiple blobs' metadata and sliver pairs on a node, and requests storage confirmations.
    /// Returns a vector of NodeResults, maintaining the same interface as store_metadata_and_pairs.
    pub async fn store_blobs_batch(
        &self,
        bundles: Vec<MultiPutBundle>,
    ) -> Vec<NodeResult<SignedStorageConfirmation, StoreError>> {
        // Implementation will call the batch endpoint
        // Each bundle gets a NodeResult with proper weight
    }
}
```

**Key Points:**
- Reuses existing `NodeResult<SignedStorageConfirmation, StoreError>` type
- Same weight calculation as individual requests (number of shards)
- Maintains compatibility with existing error handling and retry logic
- Client code can treat batch results the same as individual results

**Test:** Serialization/deserialization tests, edge case handling

---

### Phase 3: Implement Server-side Multi-put Endpoint
**Files:**
- `crates/walrus-service/src/node/server/routes.rs`
- `crates/walrus-service/src/node.rs`
- `crates/walrus-storage-node-client/src/client.rs`

#### 3.1 Add new route
- Route: `POST /v1/blobs/batch`
- URL template constant: `BATCH_BLOBS_URL_TEMPLATE`

#### 3.2 Implement handler
```rust
async fn handle_multi_put(
    &self,
    request: MultiPutRequest,
) -> Result<MultiPutResponse, Error> {
    let mut results = Vec::new();

    for bundle in request.blob_bundles {
        let result = if self.is_blob_registered(&bundle.blob_id)? {
            // Blob registered, store all slivers directly
            let mut processed = Vec::new();
            let mut failed = Vec::new();

            // Store metadata if provided
            if let Some(metadata) = bundle.metadata {
                self.store_metadata(metadata.into()).await?;
            }

            // Store each sliver pair
            for pair in bundle.sliver_pairs {
                match self.store_sliver_pair(&bundle.blob_id, pair).await {
                    Ok(_) => processed.push(pair.index()),
                    Err(e) => failed.push((pair.index(), e.to_string())),
                }
            }

            BlobStoreResult {
                blob_id: bundle.blob_id,
                status: if failed.is_empty() {
                    StoreStatus::Stored
                } else {
                    StoreStatus::PartiallyStored {
                        stored: processed.len(),
                        cached: 0
                    }
                },
                processed_pairs: processed,
                failed_pairs: failed,
                error: None,
            }
        } else {
            // Cache everything for later processing
            self.cache_blob_slivers(
                bundle.blob_id,
                bundle.sliver_pairs.clone(),
                bundle.metadata.map(Into::into),
            ).await?;

            BlobStoreResult {
                blob_id: bundle.blob_id,
                status: StoreStatus::Cached,
                processed_pairs: bundle.sliver_pairs.iter()
                    .map(|p| p.index())
                    .collect(),
                failed_pairs: Vec::new(),
                error: None,
            }
        };
        results.push(result);
    }

    Ok(MultiPutResponse { results })
}
```

#### 3.3 Add client method
```rust
pub async fn store_blob_batch(
    &self,
    request: MultiPutRequest,
) -> Result<MultiPutResponse, NodeError>
```

**Test:** Integration test with multiple blobs, mixed registration states

---

### Phase 4: Modify Client to Batch Slivers by Node
**Files:**
- `crates/walrus-sdk/src/client.rs`
- `crates/walrus-sdk/src/client/communication/node.rs`

#### 4.1 Add batched certificate collection method
```rust
async fn get_all_blob_certificates_batched<'a, T>(
    &'a self,
    blobs_to_certify: Vec<WalrusStoreBlob<'a, T>>,
    store_args: &StoreArgs,
) -> ClientResult<Vec<WalrusStoreBlob<'a, T>>> {
    // Group all blobs' slivers by target node
    let mut bundles_by_node: HashMap<NodeIndex, Vec<BlobBundle>> = HashMap::new();

    for blob in &blobs_to_certify {
        let (pairs, metadata) = blob.get_encoded_data()?;
        let pairs_per_node = self.pairs_per_node(
            metadata.blob_id(),
            &pairs,
            &committees
        ).await;

        for (node_index, node_pairs) in pairs_per_node {
            bundles_by_node.entry(node_index)
                .or_default()
                .push(BlobBundle {
                    blob_id: *metadata.blob_id(),
                    metadata: Some(metadata.clone().into()),
                    sliver_pairs: node_pairs.into_iter().cloned().collect(),
                });
        }
    }

    // Send batch requests in parallel with progress tracking
    let multi_pb = Arc::new(MultiProgress::new());
    let futures = bundles_by_node.into_iter().map(|(node_idx, bundles)| {
        let pb = multi_pb.add(styled_progress_bar(bundles.len()));
        async move {
            let result = self.communication_factory
                .get_node(node_idx)?
                .store_blob_batch(MultiPutRequest { blob_bundles: bundles })
                .await;
            pb.finish();
            result
        }
    });

    let batch_results = futures::future::join_all(futures).await;

    // Process results and get certificates
    self.collect_certificates_for_batched_blobs(
        blobs_to_certify,
        batch_results,
        store_args
    ).await
}
```

#### 4.2 Add fallback mechanism
```rust
// In NodeWriteCommunication
pub async fn store_blob_batch_with_fallback(
    &self,
    bundles: Vec<BlobBundle>,
) -> Vec<BlobStoreResult> {
    // Try batch endpoint first
    match self.client.store_blob_batch(...).await {
        Ok(response) => response.results,
        Err(_) => {
            // Fallback to individual requests
            self.store_bundles_individually(bundles).await
        }
    }
}
```

**Test:** End-to-end test comparing batch vs individual performance

---

### Phase 5: Update Event Processor to Check Cache
**Files:**
- `crates/walrus-service/src/node/blob_event_processor.rs`
- `crates/walrus-service/src/node.rs`

```rust
impl StorageNodeInner {
    async fn process_register_event(&self, event: BlobRegistered) -> Result<()> {
        // Existing registration logic...
        self.storage.register_blob(event.blob_id, event.epoch)?;

        // Process any cached slivers for this blob
        if let Some(cached) = self.pre_registered_cache.get(&event.blob_id)? {
            let mut stored_count = 0;

            // Store metadata if not already present
            if cached.metadata.is_some() && !self.storage.has_metadata(&event.blob_id)? {
                self.storage.store_metadata(cached.metadata.unwrap()).await?;
            }

            // Store all cached sliver pairs
            for pair in &cached.sliver_pairs {
                // Store primary sliver
                if self.store_sliver_unchecked(
                    cached.metadata.as_ref().unwrap(),
                    pair.index(),
                    Sliver::Primary(pair.primary.clone())
                ).await? {
                    stored_count += 1;
                }

                // Store secondary sliver
                if self.store_sliver_unchecked(
                    cached.metadata.as_ref().unwrap(),
                    pair.index(),
                    Sliver::Secondary(pair.secondary.clone())
                ).await? {
                    stored_count += 1;
                }
            }

            // Remove from cache after successful processing
            self.pre_registered_cache.remove(&event.blob_id)?;

            tracing::info!(
                blob_id = %event.blob_id,
                stored_count,
                "processed cached slivers after register event"
            );

            self.metrics.cached_blobs_processed.inc();
            self.metrics.cached_slivers_stored.add(stored_count as i64);
        }

        Ok(())
    }
}
```

**Test:**
- Test with delayed register events
- Test cache expiration before register event
- Test concurrent register events and cache writes

---

### Phase 6: Add Monitoring and Metrics
**Files:** `crates/walrus-service/src/node/metrics.rs`

#### New metrics to add:
- `cached_blobs_total`: Gauge of currently cached blobs
- `cached_slivers_total`: Gauge of total cached slivers
- `multi_put_requests_total`: Counter of batch requests
- `multi_put_blobs_per_request`: Histogram of blobs per batch
- `multi_put_slivers_per_request`: Histogram of slivers per batch
- `cache_hit_rate`: Gauge (processed from cache / total processed)
- `sliver_upload_latency_ms`: Histogram with labels for single/batch
- `cache_memory_bytes`: Gauge of cache memory usage
- `cache_expired_blobs_total`: Counter of expired blobs

**Test:** Verify all metrics are collected correctly

---

### Phase 7: Add Integration Tests
**Files:** `crates/walrus-e2e-tests/tests/test_batch_upload.rs`

#### Test scenarios:
1. **Happy path:** Multiple blobs with immediate register events
2. **Cache flow:** Slivers cached, then register events arrive
3. **Mixed state:** Some blobs registered, some not
4. **Cache expiration:** Slivers expire before register event
5. **Partial failure:** Some nodes fail, ensure others succeed
6. **Backward compatibility:** Old client with new server, new client with old server
7. **Large batch:** 100+ blobs in single batch
8. **Network interruption:** Batch request interrupted midway
9. **Concurrent operations:** Multiple clients sending batches

---

### Phase 8: Performance Testing and Optimization

#### 8.1 Benchmarks to create:
- Single blob upload vs batch of 10, 50, 100 blobs
- Cache memory usage under load
- Time from upload to certificate with/without cache
- Network bandwidth usage comparison

#### 8.2 Optimization targets:
- Adaptive batch sizing based on blob sizes
- Dynamic cache TTL based on register event patterns
- Compression for large batches
- Connection pooling optimization

#### 8.3 Performance goals:
- 50-70% latency reduction for 10+ blob uploads
- < 100MB cache memory per node
- No increase in failure rate
- Support for 1000+ concurrent cached blobs

**Test:** Load testing with production-like workloads

---

## Key Implementation Notes

### Backward Compatibility
- Server supports both `/v1/blobs/batch` and individual endpoints
- Client detects batch support via version check or trial request
- Graceful fallback ensures no disruption

### Error Handling
- Partial failures don't block successful blobs
- Clear error messages indicate cache vs storage failures
- Retry logic accounts for cached vs stored state

### Security Considerations
- Cache size limits prevent DoS
- TTL prevents indefinite resource consumption
- All cached data validated when register event arrives

### Monitoring
- Dashboard to track cache efficiency
- Alerts for high cache miss rate
- Tracking of latency improvements

## Benefits of This Design

1. **Efficient batching:** Each blob's slivers are grouped together, reducing overhead
2. **Single metadata copy:** Avoids duplicating metadata across sliver pairs
3. **Atomic blob operations:** Can process all slivers for a blob together
4. **Better cache management:** Easier to expire/remove all data for a blob at once
5. **Reduced network overhead:** Fewer redundant metadata transmissions
6. **Improved latency:** Parallel processing and elimination of registration delays

## Success Metrics

- **Primary Goal:** 50-70% reduction in upload latency for multi-blob operations
- **Secondary Goals:**
  - Maintain current reliability levels
  - Support for 1000+ concurrent uploads
  - Cache memory usage < 100MB per node
  - Zero disruption to existing clients

## Rollout Strategy

1. **Phase 1-2:** Internal development and testing (Week 1-2)
2. **Phase 3-5:** Server-side deployment with backward compatibility (Week 3-4)
3. **Phase 6-7:** Client rollout with feature flag (Week 5)
4. **Phase 8:** Performance validation and optimization (Week 6)
5. **Production rollout:** Gradual enablement with monitoring (Week 7+)
