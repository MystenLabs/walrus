# Chunked Upload Middleware Design

## Overview

The chunked upload middleware enables resumable uploads of large blobs to the Walrus publisher daemon by intercepting PUT requests with chunked upload
headers, accumulating chunks, and forwarding complete blobs to the standard `put_blob` handler.

A request is treated as a chunked upload if it contains the `X-Upload-Session-Id` header. Requests without this header are passed through unchanged.

## Architecture

### Components

1. ChunkedUploadMiddleware: Axum middleware that intercepts requests
2. ChunkedUploadState: Manages upload sessions and storage
3. UploadSession: Tracks session metadata and chunk status
4. Background cleanup task: Periodically removes expired sessions

### Request Flow

```
Client Request → Middleware → Session Management → Storage → Completion Check
                    ↓
             Incomplete: Return 202 Accepted (JSON status)
                    ↓
             Complete: Cleanup + Forward to put_blob → Return 200 OK
```

## Session Management

### Session Lifecycle

1. Creation: First chunk creates session with metadata
2. Accumulation: Chunks are written at fixed offsets; order does not matter
3. Completion: When all chunks are received, session is cleaned up and the blob is forwarded
4. Expiration: Background task removes sessions older than timeout

### Storage Layout

```
{storage_dir}/
├── {session_id}.data    # Binary chunk data
└── {session_id}.json    # Session metadata
```

### Persistence

- Sessions persist across daemon restarts via disk storage
- On startup, existing sessions are restored from metadata files
- Corrupted or invalid metadata files are automatically cleaned up

## Integration with put_blob

The middleware acts as a transparent proxy:

1. Intercepts chunked upload requests (identified by headers)
2. Accumulates chunks until the upload is complete
3. Reconstructs the original PUT request with the complete blob data
4. Removes chunked upload headers
5. Forwards to the existing put_blob handler unchanged

## Headers Protocol

| Header                  | Purpose                                 | Example       |
| ----------------------- | --------------------------------------- | ------------- |
| `X-Upload-Session-Id`   | Unique session identifier               | `uuid4()`     |
| `X-Upload-Chunk-Index`  | Zero-based chunk number                 | `0`, `1`, `2` |
| `X-Upload-Total-Chunks` | Total expected chunks                   | `10`          |
| `X-Upload-Chunk-Size`   | Expected uniform chunk size (non-final) | `1048576`     |
| `X-Upload-Total-Size`   | Complete blob size                      | `10485760`    |

Notes:

- All headers above are required for chunked uploads.
- `X-Upload-Chunk-Size` is the session’s expected chunk size for all non-final chunks and must remain constant across the session. The final chunk may
  be smaller than this value.

## Response Codes

- 202 Accepted: Chunk received, upload in progress (returns JSON status including `missing_chunks`)
- 200 OK: Upload complete, blob stored successfully (standard publisher response)
- 400 Bad Request: Invalid/missing headers or size mismatches
- 404 Not Found: Session not found
- 413 Payload Too Large: Size limits exceeded
- 500 Internal Server Error: Unexpected server error

## Missing Chunk Detection

For in-progress uploads, the middleware returns `202 Accepted` with a JSON body that includes any missing chunks up to the highest received chunk
index. Example:

```json
{
  "session_id": "D4DAE921-CC3D-4509-B0A3-DC6FF6567752",
  "chunks_received": 3,
  "total_chunks": 5,
  "complete": false,
  "missing_chunks": [1, 3]
}
```

## Background Cleanup

A background task periodically:

1. Checks all active sessions for expiration
2. Removes expired sessions from memory
3. Deletes associated data and metadata files
4. Logs cleanup operations

By default this runs every 5 minutes (configurable). The task starts automatically with daemon initialization and handles missed ticks gracefully.

## Configuration

```rust
ChunkedUploadConfig {
    storage_dir: PathBuf::from("/tmp/walrus_uploads"),
    session_timeout: Duration::from_secs(3600),  // 1 hour
    cleanup_interval: Duration::from_secs(300),  // default 5 minutes
    max_total_size: 100 * 1024 * 1024 * 1024,   // 100 GiB
}
```

Notes:

- `max_total_size` is enforced for the complete blob; requests with `total_size` greater than this are rejected.
- Requests with `chunk_size` greater than `max_total_size` are rejected.

## Size and Consistency Constraints

To ensure reliable reconstruction of blobs from fixed offsets, the implementation enforces the following:

- `X-Upload-Chunk-Size` must be identical for all requests in the session (it defines the uniform chunk size)
- Non-final chunks must be exactly `chunk_size` bytes
- The final chunk may be smaller (the remaining bytes so that the sum equals `total_size`)
- Session parameters (`total_chunks`, `total_size`, `chunk_size`) must remain constant across requests; mismatches are rejected
- Guards exist against invalid combinations (zero sizes/chunks, too many chunks vs. total size)

## Security & Reliability

- Resumability: Clients can resume interrupted uploads using the same session ID
- Idempotency: Re-sending a previously stored chunk is accepted
- Limits: Total size limit enforced; oversized chunks are rejected
- Timeout protection: Expired sessions are automatically cleaned up
- Error handling: Graceful handling of disk I/O errors and corruption
- Authentication: Inherits existing JWT and other security features from the publisher endpoint

## Testing

Included tests cover:

- Session creation
- Chunk storage and completion
- Session persistence and restoration across restarts
- Background cleanup of expired sessions
- Missing chunk detection
- Chunk size enforcement
