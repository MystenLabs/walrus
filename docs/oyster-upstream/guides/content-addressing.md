# Content Addressing

Oyster uses **content addressing** to identify blobs. A blob's
identity is derived from its contents, not from where it is stored. This
enables deduplication, integrity verification, and content-based retrieval.

## What is a blob ID?

When you upload data to Oyster, the server computes a **BLAKE2s-256 hash**
of the raw bytes. This produces a 64-character hex string called the
**blob ID**:

```
2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824
```

This blob ID is returned in the upload response and stored alongside the
object metadata. The same data always produces the same blob ID, regardless
of which bucket or key it's stored under.

## Deduplication

If you upload the same file to two different keys or two different buckets,
Oyster recognizes that the content is identical and stores the data **only
once**. For example:

```bash
# Upload the same file to two different keys
curl -s -X PUT -H "Authorization: Bearer $API_KEY" \
  --data-binary @photo.png \
  "$OYSTER_URL/api/v1/buckets/bucket-a/blobs/photo.png" | jq .blob_id

curl -s -X PUT -H "Authorization: Bearer $API_KEY" \
  --data-binary @photo.png \
  "$OYSTER_URL/api/v1/buckets/bucket-b/blobs/copy.png" | jq .blob_id
```

Both uploads return the **same `blob_id`** because the content is
identical. The underlying storage holds only one copy.

## Reference counting

Each key-to-blob mapping is a **reference**. Oyster tracks how many
references point to each blob ID:

```
bucket-a/photo.png  →  blob_id: abc123...  (ref count: 2)
bucket-b/copy.png   →  blob_id: abc123...
```

When you delete a key, Oyster removes that reference. The physical blob
data is only deleted from storage when **all references are removed**:

```bash
# Delete one reference — blob data still exists
curl -s -X DELETE -H "Authorization: Bearer $API_KEY" \
  "$OYSTER_URL/api/v1/buckets/bucket-a/blobs/photo.png"
# ref count: 1 — data preserved

# Delete the last reference — blob data is removed
curl -s -X DELETE -H "Authorization: Bearer $API_KEY" \
  "$OYSTER_URL/api/v1/buckets/bucket-b/blobs/copy.png"
# ref count: 0 — data deleted from storage
```

This means deleting a key in one bucket never affects the same content
stored under a different key or bucket.

## Reading by blob ID

You can retrieve any blob by its content-addressed ID, without knowing
which bucket or key it belongs to:

```bash
curl -s "$OYSTER_URL/api/v1/blobs/by-blob-id/2cf24dba5fb0a30e..."
```

This is a **public endpoint** with no authentication required. It always
returns the content with `Content-Type: application/octet-stream`.

This is useful when you have stored a blob ID externally (for example, in a
database or onchain) and want to retrieve the data directly.

## Practical implications

- **Storage efficiency**: identical files across buckets cost no extra
  storage.
- **Safe deletion**: deleting a key never destroys data that other keys
  depend on.
- **Integrity**: the blob ID serves as a checksum; if the data is
  corrupted, the hash does not match.
- **Immutable content**: a given blob ID always maps to the same data.
  Overwriting a key creates a new blob with a new blob ID.
