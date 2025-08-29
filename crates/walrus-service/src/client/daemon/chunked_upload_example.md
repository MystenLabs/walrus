# Chunked Upload Middleware for Large Blob Uploads

The chunked upload middleware enables resumable uploads of large blobs to the Walrus publisher
daemon. This allows clients to upload large files reliably, with the ability to resume interrupted
uploads.

## How it Works

The middleware intercepts requests with specific chunked upload headers, accumulates chunks until a
complete blob is received, then forwards the reconstructed blob to the standard `put_blob` handler.

### Headers

The following headers control the chunked upload process:

- `X-Upload-Session-Id`: Unique identifier for the upload session (required)
- `X-Upload-Chunk-Index`: Zero-based index of this chunk (0, 1, 2, ...) (required)
- `X-Upload-Total-Chunks`: Total number of chunks expected (required)
- `X-Upload-Chunk-Size`: Size of this specific chunk in bytes (required)
- `X-Upload-Total-Size`: Total size of the complete blob (required)
- `Content-Type`: Must be `application/octet-stream`

## Client Usage Example

Here's how a client would upload a large blob using chunked uploads:

### Python Example

```python
import uuid
import math
import requests

def upload_large_blob(file_path, publisher_url, chunk_size=10*1024*1024):
    """Upload a large blob using chunked uploads."""

    # Generate unique session ID
    session_id = str(uuid.uuid4())

    # Calculate chunk information
    file_size = os.path.getsize(file_path)
    total_chunks = math.ceil(file_size / chunk_size)

    print(f"Uploading {file_path} ({file_size} bytes) in {total_chunks} chunks")

    with open(file_path, 'rb') as f:
        for chunk_index in range(total_chunks):
            # Read chunk
            chunk_data = f.read(chunk_size)
            actual_chunk_size = len(chunk_data)

            # Prepare headers
            headers = {
                'Content-Type': 'application/octet-stream',
                'X-Upload-Session-Id': session_id,
                'X-Upload-Chunk-Index': str(chunk_index),
                'X-Upload-Total-Chunks': str(total_chunks),
                'X-Upload-Chunk-Size': str(actual_chunk_size),
                'X-Upload-Total-Size': str(file_size),
            }

            # Upload chunk
            response = requests.put(
                f"{publisher_url}/v1/blobs",
                data=chunk_data,
                headers=headers
            )

            if response.status_code == 202:
                # Chunk accepted, continue
                result = response.json()
                print(f"Chunk {chunk_index + 1}/{total_chunks} uploaded "
                      f"({result['chunks_received']}/{result['total_chunks']})")
            elif response.status_code == 200:
                # Upload complete!
                result = response.json()
                print(f"Upload completed! Blob ID: {result['blob_id']}")
                return result
            else:
                print(f"Upload failed: {response.status_code} {response.text}")
                return None

    return None
```

### Bash/curl Example

```bash
#!/bin/bash

# Configuration
FILE_PATH="/path/to/large/file.bin"
PUBLISHER_URL="http://localhost:31415"
CHUNK_SIZE=10485760  # 10MB chunks

# Generate session ID
SESSION_ID=$(uuidgen)
FILE_SIZE=$(stat -c%s "$FILE_PATH")
TOTAL_CHUNKS=$(( (FILE_SIZE + CHUNK_SIZE - 1) / CHUNK_SIZE ))

echo "Uploading $FILE_PATH ($FILE_SIZE bytes) in $TOTAL_CHUNKS chunks"

# Upload each chunk
for ((chunk_index=0; chunk_index<TOTAL_CHUNKS; chunk_index++)); do
    offset=$((chunk_index * CHUNK_SIZE))

    # Extract chunk (last chunk may be smaller)
    if [ $chunk_index -eq $((TOTAL_CHUNKS - 1)) ]; then
        chunk_size=$((FILE_SIZE - offset))
    else
        chunk_size=$CHUNK_SIZE
    fi

    # Upload chunk
    response=$(dd if="$FILE_PATH" bs=1 skip=$offset count=$chunk_size 2>/dev/null | \
        curl -s -w "%{http_code}" \
        -X PUT "$PUBLISHER_URL/v1/blobs" \
        -H "Content-Type: application/octet-stream" \
        -H "X-Upload-Session-Id: $SESSION_ID" \
        -H "X-Upload-Chunk-Index: $chunk_index" \
        -H "X-Upload-Total-Chunks: $TOTAL_CHUNKS" \
        -H "X-Upload-Chunk-Size: $chunk_size" \
        -H "X-Upload-Total-Size: $FILE_SIZE" \
        --data-binary @-)

    http_code="${response: -3}"
    response_body="${response%???}"

    if [ "$http_code" = "202" ]; then
        echo "Chunk $((chunk_index + 1))/$TOTAL_CHUNKS uploaded"
    elif [ "$http_code" = "200" ]; then
        echo "Upload completed! Response: $response_body"
        break
    else
        echo "Upload failed: HTTP $http_code - $response_body"
        exit 1
    fi
done
```

## Resumability

If an upload is interrupted, clients can resume by:

1. Using the same `X-Upload-Session-Id`
2. Re-sending any missing chunks
3. The middleware will accept already-received chunks and continue

## Configuration

The middleware can be configured with:

```rust
let chunked_upload_config = ChunkedUploadConfig {
    storage_dir: PathBuf::from("/tmp/walrus_uploads"),
    session_timeout: Duration::from_secs(3600),  // 1 hour
    max_total_size: 100 * 1024 * 1024 * 1024,   // 100GB
    max_chunk_size: 10 * 1024 * 1024,           // 10MB
};
```

## Response Codes

- `202 Accepted`: Chunk received, upload still in progress
- `200 OK`: Upload completed successfully, returns standard blob response
- `400 Bad Request`: Invalid headers or chunk data
- `404 Not Found`: Upload session not found
- `413 Payload Too Large`: Chunk or total size exceeds limits
- `500 Internal Server Error`: Server error during processing

## Error Handling

Clients should implement retry logic for:
- Network failures
- 5xx server errors
- Timeouts

The middleware is idempotent - re-sending the same chunk multiple times is safe.

## Security Considerations

The chunked upload middleware inherits all security features from the existing publisher:
- JWT authentication (if configured)
- Size limits
- Rate limiting
- CORS headers

Session files are stored temporarily and cleaned up after completion or timeout.
