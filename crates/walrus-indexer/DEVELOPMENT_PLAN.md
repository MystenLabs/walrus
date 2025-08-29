# Walrus Indexer Development Plan

## Current State
✅ **INTEGRATION COMPLETE**: Checkpoint downloader and event processor successfully integrated into walrus-indexer crate.

**Build Status**: Compiles successfully with `cargo build`
**Last Updated**: 2024-08-29

## Phase 1: Core Event Processing (NEXT)

### 1.1 Event Parsing Implementation 🔥 HIGH PRIORITY
**Location**: `src/event_processor/checkpoint.rs::extract_index_operation()`
**Current Status**: Placeholder implementation
**Required**:
- [ ] Research actual Walrus Move package event structures
- [ ] Implement event type matching (e.g., "IndexAdded", "IndexRemoved")
- [ ] Parse event data to extract:
  - `bucket_id: ObjectID`
  - `primary_key: String`
  - `blob_id: BlobId`
  - `secondary_indices: Vec<(String, String)>`
- [ ] Handle event parsing errors gracefully

**Code Template**:
```rust
fn extract_index_operation(event: &Event, _checkpoint: u64) -> Option<IndexOperation> {
    if event.package_id != self.walrus_package_id {
        return None;
    }

    match event.type_.to_canonical_string().as_str() {
        "IndexAdded" => {
            // Parse event.contents to extract data
            Some(IndexOperation::IndexAdded { /* ... */ })
        }
        "IndexRemoved" => {
            // Parse event.contents to extract data
            Some(IndexOperation::IndexRemoved { /* ... */ })
        }
        _ => None,
    }
}
```

### 1.2 CLI Configuration Enhancement
**Location**: `src/bin/indexer.rs`
**Current Status**: Basic args, event processor config disabled
**Required**:
- [ ] Add `--walrus-package-id` CLI argument
- [ ] Add `--enable-event-processing` flag
- [ ] Add event processor database path configuration
- [ ] Add checkpoint downloader tuning parameters

**Code Template**:
```rust
#[derive(Parser, Debug)]
struct IndexerArgs {
    // ... existing args

    /// Walrus package ID for event filtering
    #[arg(long)]
    walrus_package_id: Option<String>,

    /// Enable event processing from Sui blockchain
    #[arg(long)]
    enable_event_processing: bool,

    /// Event processor database path
    #[arg(long, default_value = "./indexer-db/events")]
    event_db_path: String,
}
```

### 1.3 Error Recovery & Resumption
**Location**: `src/event_processor/processor.rs`
**Current Status**: Basic checkpoint tracking
**Required**:
- [ ] Implement checkpoint gap detection
- [ ] Add recovery from last processed checkpoint
- [ ] Handle network disconnection gracefully
- [ ] Add exponential backoff for failed operations

## Phase 2: Production Readiness

### 2.1 Testing Infrastructure
**Required**:
- [ ] Unit tests for event parsing logic
- [ ] Integration tests with mock Sui events
- [ ] Property-based tests for index operations
- [ ] Performance benchmarks for event processing

### 2.2 Monitoring & Observability
**Location**: `src/event_processor/metrics.rs`
**Current Status**: Basic metrics structure
**Required**:
- [ ] Add processing latency metrics
- [ ] Add error rate tracking
- [ ] Add checkpoint lag monitoring
- [ ] Add database size metrics

### 2.3 Configuration Validation
**Required**:
- [ ] Validate Walrus package ID format
- [ ] Check RPC endpoint connectivity
- [ ] Verify database permissions
- [ ] Add configuration file support (TOML/JSON)

## Phase 3: Advanced Features

### 3.1 Committee Verification
**Location**: `src/event_processor/checkpoint.rs`
**Current Status**: Simplified checkpoint creation
**Required**:
- [ ] Implement proper checkpoint verification
- [ ] Add committee store management
- [ ] Handle committee changes
- [ ] Add bootstrap committee logic

### 3.2 Catchup Optimization
**Required**:
- [ ] Implement event blob catchup for large gaps
- [ ] Add parallel checkpoint processing
- [ ] Optimize batch event processing
- [ ] Add configurable worker pools

### 3.3 High Availability
**Required**:
- [ ] Add health check endpoints
- [ ] Implement graceful shutdown
- [ ] Add leader election for multiple instances
- [ ] Support read-only mode

## Implementation Priority Queue

### 🔥 Critical (Week 1)
1. **Event Parsing Logic** - Core functionality blocker
2. **CLI Configuration** - User experience essential
3. **Error Recovery** - Production stability

### 🔶 High (Week 2-3)
1. **Testing Infrastructure** - Quality assurance
2. **Monitoring Setup** - Operational visibility
3. **Configuration Validation** - User safety

### 🔵 Medium (Week 4+)
1. **Committee Verification** - Security enhancement
2. **Catchup Optimization** - Performance improvement
3. **High Availability** - Scalability feature

## Technical Debt

### Known Issues
1. **Placeholder Event Parsing** - `checkpoint.rs:119-149`
2. **Simplified Checkpoint Verification** - `processor.rs:227-229`
3. **Unused Event Sender** - `processor.rs:58` (warning)
4. **Private Interface Warning** - `indexer.rs:58` (visibility)

### Refactoring Opportunities
1. **Extract Event Parsing Module** - Separate concern from checkpoint processing
2. **Configuration Builder Pattern** - Improve ergonomics
3. **Error Type Consolidation** - Unified error handling
4. **Async Task Management** - Better resource cleanup

## Testing Strategy

### Unit Tests
- Event parsing with various event types
- Configuration validation logic
- Database store operations
- Metrics collection accuracy

### Integration Tests
- End-to-end event processing pipeline
- Network failure recovery scenarios
- Database migration/upgrade paths
- API endpoint functionality

### Performance Tests
- Checkpoint processing throughput
- Memory usage under load
- Database query performance
- Event channel backpressure handling

## Deployment Considerations

### System Requirements
- RocksDB storage permissions
- Network access to Sui RPC endpoints
- Sufficient disk space for checkpoint data
- Memory for event processing buffers

### Configuration Management
- Environment-specific package IDs
- RPC endpoint failover configuration
- Database path management
- Logging level configuration

### Monitoring Integration
- Prometheus metrics export
- Grafana dashboard templates
- Alert rules for critical failures
- Log aggregation setup

## Resources & References

### Documentation
- Sui Checkpoint API documentation
- Walrus Move package event schemas
- RocksDB configuration tuning guides
- Tokio async patterns and best practices

### Code References
- `walrus-service/src/event/event_processor/` - Original implementation
- `checkpoint-downloader/` - Parallel checkpoint fetching
- `typed-store/` - Database abstraction patterns
- `walrus-utils/metrics.rs` - Metrics collection patterns

---

**Last Updated**: 2024-08-29
**Next Review**: When starting Phase 1 work
**Status**: Ready for implementation
