# Walrus Index System Architecture

## System Overview

```mermaid
graph TB
    subgraph "Sui Blockchain"
        SC[Smart Contracts]
        EV[Events]
        SC --> EV
    end
    
    subgraph "Client Applications"
        CL1[Web App]
        CL2[CLI Tool]
        CL3[SDK]
    end
    
    subgraph "Storage Nodes"
        SN1[Node 1]
        SN2[Node 2]
        SN3[Node N]
    end
    
    subgraph "Indexer (Octopus Index)"
        EP[Event Processor]
        DS[Dual Index Store]
        API[REST API]
        CACHE[Cache Layer]
        
        EP --> DS
        API --> CACHE
        CACHE --> DS
    end
    
    EV -.->|Subscribe| EP
    CL1 --> API
    CL2 --> API
    CL3 --> API
    CL1 --> SN1
    CL2 --> SN2
    CL3 --> SN3
```

## Component Details

### 1. Sui Blockchain

**Smart Contracts:**
- Manages buckets and blob metadata
- Emits events for index operations
- Stores resource allocation

**Events Emitted:**
```rust
// Index mutation events
event IndexAdded {
    bucket_id: ObjectID,
    identifier: String,
    object_id: ObjectID,
    blob_id: BlobId,
}

event IndexRemoved {
    object_id: ObjectID,
}
```

**APIs:**
- Transaction submission
- Event subscription
- State queries

---

### 2. Client Applications

**Operations:**
1. Create bucket
2. Store blob with index
3. Read blob by identifier
4. Read blob by object_id
5. Delete blob index

**Client-Side Data Structures:**
```rust
struct BlobRequest {
    bucket_id: ObjectID,
    identifier: String,
    data: Vec<u8>,
}

struct BlobResponse {
    blob_id: BlobId,
    object_id: ObjectID,
    identifier: String,
}
```

**APIs Used:**
- Indexer REST API for discovery
- Storage Node API for blob data
- Sui RPC for transactions

---

### 3. Storage Nodes (Unchanged)

**Responsibilities:**
- Store blob slivers
- Serve blob data
- Generate availability certificates

**APIs:**
- `PUT /v1/store` - Store blob
- `GET /v1/blobs/{blob_id}` - Retrieve blob
- `GET /v1/availability/{blob_id}` - Check availability

---

### 4. Indexer (Octopus Index) - NEW

**Core Data Structures:**

```rust
// Dual Index System
pub struct OctopusIndexStore {
    // Primary: bucket_id/identifier -> BlobIdentity
    primary_index: DBMap<String, PrimaryIndexValue>,
    // Object: object_id -> bucket_id/identifier
    object_index: DBMap<String, ObjectIndexValue>,
}

pub struct BlobIdentity {
    pub blob_id: BlobId,
    pub object_id: ObjectID,
}

pub struct PrimaryIndexValue {
    pub blob_identity: BlobIdentity,
}

pub struct ObjectIndexValue {
    pub bucket_id: ObjectID,
    pub identifier: String,
}

pub enum IndexMutation {
    Insert {
        bucket_id: ObjectID,
        identifier: String,
        object_id: ObjectID,
        blob_id: BlobId,
    },
    Delete {
        object_id: ObjectID,
    },
}
```

**REST API Endpoints:**

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/v1/blobs/{bucket_id}/{identifier}` | Get blob by bucket + identifier |
| GET | `/v1/object/{object_id}` | Get blob by object ID |
| GET | `/v1/bucket/{bucket_id}` | List all blobs in bucket |
| GET | `/v1/bucket/{bucket_id}/{prefix}` | List blobs with prefix |
| GET | `/v1/bucket/{bucket_id}/stats` | Get bucket statistics |
| GET | `/v1/health` | Health check |

---

## Data Flow Sequences

### 1. Store Blob with Index

```mermaid
sequenceDiagram
    participant Client
    participant Sui
    participant Indexer
    participant Storage
    
    Client->>Sui: Create index transaction
    Sui->>Sui: Store metadata
    Sui-->>Indexer: Emit IndexAdded event
    
    Client->>Storage: Upload blob data
    Storage->>Client: Return availability cert
    
    Client->>Sui: Submit cert
    
    Indexer->>Indexer: Process event
    Indexer->>Indexer: Update dual indices
    
    Note over Indexer: Primary: bucket/id → blob
    Note over Indexer: Object: object_id → bucket/id
```

### 2. Read Blob by Identifier

```mermaid
sequenceDiagram
    participant Client
    participant Indexer
    participant Storage
    
    Client->>Indexer: GET /v1/blobs/{bucket}/{id}
    Indexer->>Indexer: Query primary index
    Indexer->>Client: Return blob_id, object_id
    
    Client->>Storage: GET /v1/blobs/{blob_id}
    Storage->>Client: Return blob data
```

### 3. Read Blob by Object ID

```mermaid
sequenceDiagram
    participant Client
    participant Indexer
    participant Storage
    
    Client->>Indexer: GET /v1/object/{object_id}
    Indexer->>Indexer: Query object index
    Indexer->>Indexer: Then query primary index
    Indexer->>Client: Return blob_id, identifier
    
    Client->>Storage: GET /v1/blobs/{blob_id}
    Storage->>Client: Return blob data
```

### 4. Delete Index Entry

```mermaid
sequenceDiagram
    participant Client
    participant Sui
    participant Indexer
    
    Client->>Sui: Delete index transaction
    Sui-->>Indexer: Emit IndexRemoved event
    
    Indexer->>Indexer: Query object index
    Indexer->>Indexer: Get bucket_id, identifier
    Indexer->>Indexer: Delete from both indices
    
    Note over Indexer: Atomic deletion
```

---

## Key Design Decisions

### Dual Index System

**Primary Index:**
- Key: `{bucket_id}/{identifier}`
- Value: `BlobIdentity{blob_id, object_id}`
- Use case: User knows bucket and path

**Object Index:**
- Key: `{object_id}`
- Value: `{bucket_id, identifier}`
- Use case: User only has object_id

### Atomic Operations

All index mutations use RocksDB batch operations:
```rust
let mut batch = self.primary_index.batch();
batch.insert_batch(&self.primary_index, [(key1, val1)])?;
batch.insert_batch(&self.object_index, [(key2, val2)])?;
batch.write()  // Atomic commit
```

### Event Processing

```rust
IndexerEventProcessor
    ├── Downloads checkpoints
    ├── Filters Walrus events
    ├── Converts to IndexOperations
    └── Applies to OctopusIndexStore
```

### Cache Strategy

- In-memory HashMap for hot entries
- Cleared on complex operations
- Write-through for consistency

---

## Performance Characteristics

| Operation | Complexity | Notes |
|-----------|------------|-------|
| Insert | O(1) | Two index writes |
| Query by identifier | O(1) | Direct primary lookup |
| Query by object_id | O(1) | Two lookups (object→primary) |
| List bucket | O(n) | Prefix scan |
| Delete by object_id | O(1) | Two index deletes |
| Delete bucket | O(n) | Scan + batch delete |

---

## Storage Layout

```
RocksDB
├── octopus_index_primary/
│   ├── {bucket1}/{path1} → BlobIdentity
│   ├── {bucket1}/{path2} → BlobIdentity
│   └── {bucket2}/{path3} → BlobIdentity
│
└── octopus_index_object/
    ├── {object_id1} → {bucket1, path1}
    ├── {object_id2} → {bucket1, path2}
    └── {object_id3} → {bucket2, path3}
```

---

## Integration Points

1. **Sui Events → Indexer**: Real-time event subscription
2. **Client → Indexer**: REST API for discovery
3. **Client → Storage**: Direct blob retrieval
4. **Indexer → RocksDB**: Persistent dual indices

---

## Future Enhancements

1. **Secondary Indices**: Add custom indices per bucket
2. **Pagination**: Implement cursor-based pagination
3. **Caching**: Add Redis for distributed cache
4. **Sharding**: Horizontal scaling for large deployments
5. **Analytics**: Query patterns and usage metrics