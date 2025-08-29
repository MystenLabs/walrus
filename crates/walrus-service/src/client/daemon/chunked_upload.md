# Chunked Upload Middleware Design

## Overview

The chunked upload middleware enables resumable uploads of large blobs to the Walrus publisher daemon by intercepting PUT requests with chunked upload
headers, accumulating chunks, and forwarding complete blobs to the standard `put_blob` handler.

## Architecture

### Components

1. **ChunkedUploadMiddleware**: Axum middleware that intercepts requests
2. **ChunkedUploadState**: Manages upload sessions and storage
3. **UploadSession**: Tracks session metadata and chunk status
4. **Background Cleanup Task**: Periodically removes expired sessions

### Request Flow

```
Client Request → Middleware → Session Management → Storage → Completion Check
                     ↓
              Incomplete: Return 202 Accepted
                     ↓
              Complete: Cleanup + Forward to put_blob → Return 200 OK
```

## Session Management

### Session Lifecycle

1. **Creation**: First chunk creates session with metadata
2. **Accumulation**: Subsequent chunks append to session data file
3. **Completion**: When all chunks received, session is cleaned up and blob forwarded
4. **Expiration**: Background task removes sessions older than timeout

### Storage Layout

```
{storage_dir}/
├── {session_id}.data    # Binary chunk data
└── {session_id}.json    # Session metadata
```

### Persistence

- Sessions persist across daemon restarts via disk storage
- On startup, existing sessions are restored from metadata files
- Corrupted or incomplete sessions are automatically cleaned up

## Integration with put_blob

The middleware acts as a transparent proxy:

1. **Intercepts** chunked upload requests (identified by headers)
2. **Accumulates** chunks until upload is complete
3. **Reconstructs** original PUT request with complete blob data
4. **Removes** chunked upload headers
5. **Forwards** to existing put_blob handler unchanged

This design ensures minimal to no impact on existing blob upload functionality while adding chunked
upload support.

## Headers Protocol

| Header                  | Purpose                    | Example       |
| ----------------------- | -------------------------- | ------------- |
| `X-Upload-Session-Id`   | Unique session identifier  | `uuid4()`     |
| `X-Upload-Chunk-Index`  | Zero-based chunk number    | `0`, `1`, `2` |
| `X-Upload-Total-Chunks` | Total expected chunks      | `10`          |
| `X-Upload-Chunk-Size`   | This chunk's size in bytes | `1048576`     |
| `X-Upload-Total-Size`   | Complete blob size         | `10485760`    |

## Response Codes

- **202 Accepted**: Chunk received, upload in progress
- **200 OK**: Upload complete, blob stored successfully
- **400 Bad Request**: Invalid headers or chunk data
- **404 Not Found**: Session not found
- **413 Payload Too Large**: Size limits exceeded

## Missing Chunk Detection

When responding with `202 Accepted`, the middleware includes a `missing_chunks` array in the JSON
response that lists any missing chunks up to the highest received chunk index. This helps clients
identify and resend missing chunks for faster completion.

Example response when chunks 1 and 3 are missing (but chunks 0, 2, and 4 have been received):

```json
{
  "session_id": "abc-123",
  "chunks_received": 3,
  "total_chunks": 5,
  "complete": false,
  "missing_chunks": [1, 3]
}
```

## Background Cleanup

A tokio task runs every 5 minutes to:

1. Check all active sessions for expiration
2. Remove expired sessions from memory
3. Delete associated data and metadata files
4. Log cleanup operations

The task starts automatically with daemon initialization and handles missed ticks gracefully.

## Configuration

```rust
ChunkedUploadConfig {
    storage_dir: PathBuf::from("/tmp/walrus_uploads"),
    session_timeout: Duration::from_secs(3600),  // 1 hour
    max_total_size: 100 * 1024 * 1024 * 1024,   // 100GB
    max_chunk_size: 10 * 1024 * 1024,           // 10MB
}
```

## Chunk Size Enforcement

To ensure reliable reconstruction of blobs from chunks stored at fixed offsets, the middleware enforces immutable chunk sizes:

- **Non-final chunks**: Must be exactly the size specified in the first chunk's `X-Upload-Chunk-Size` header
- **Final chunk**: May be smaller (containing remaining bytes), calculated as `total_size - (chunks-1) * chunk_size`
- **Validation**: Each chunk is validated against expected size before storage
- **Storage**: Chunks are stored at predictable offsets: `chunk_index * chunk_size`

This ensures missing chunks can be uploaded later without data corruption or alignment issues.

## Security & Reliability

- **Resumability**: Clients can resume interrupted uploads using same session ID
- **Idempotency**: Re-sending chunks is safe
- **Size Limits**: Enforced per-chunk and total size limits
- **Chunk Size Consistency**: Enforced immutable chunk sizes (except last chunk)
- **Timeout Protection**: Expired sessions are automatically cleaned up
- **Error Handling**: Graceful handling of disk I/O errors and corruption
- **Authentication**: Inherits existing JWT and security features from put_blob

## Testing

Comprehensive test coverage includes:

- Multi-chunk upload scenarios
- Session persistence across restarts
- Background cleanup functionality
- Error conditions and edge cases
- Concurrent upload handling
