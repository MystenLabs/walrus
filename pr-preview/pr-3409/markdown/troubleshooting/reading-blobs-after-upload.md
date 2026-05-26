> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

When a blob is certified on Walrus, it does not immediately appear at every aggregator gateway. There is a short propagation window during which some aggregators may return `404 Not Found` for a blob that has been successfully certified. This is expected behavior, not a bug.

If your app reads a blob right after upload, handle this propagation window explicitly.

## Why this happens

- A blob is certified when enough storage nodes confirm storing its shards.
- Aggregator gateways read blobs from storage nodes, but each gateway maintains its own cache and connection pool to those nodes.
- Aggregators can take a few seconds to surface a newly certified blob.
- If the request hits a CDN-backed aggregator and the 404 response gets cached, the apparent unavailability can extend beyond the underlying propagation window.

## What to do

### Retry with exponential backoff

Treat post-upload 404s as transient. Non-404 errors (such as 500) indicate a different class of problem and should not be retried blindly — the example below surfaces them immediately:

```ts
async function fetchBlobWithRetry(
  blobId: string,
  aggregatorUrl: string,
  opts = { maxAttempts: 6, baseDelayMs: 500 },
): Promise<ArrayBuffer> {
  let lastError: unknown;

  for (let attempt = 0; attempt < opts.maxAttempts; attempt++) {
    const res = await fetch(`${aggregatorUrl}/v1/blobs/${blobId}`, {
      cache: "no-store",
    });

    if (res.ok) return res.arrayBuffer();

    // Surface unexpected errors immediately — don't retry server errors.
    if (res.status !== 404) {
      throw new Error(`Unexpected status ${res.status} for blob ${blobId}`);
    }

    // 404: blob may not have propagated yet. Wait, then retry.
    lastError = new Error(`404 on attempt ${attempt + 1}`);
    const delay = opts.baseDelayMs * 2 ** attempt;
    await new Promise((r) => setTimeout(r, delay));
  }

  throw new Error(
    `Blob ${blobId} not available after ${opts.maxAttempts} attempts`,
  );
}
```

Tune `maxAttempts` and `baseDelayMs` to your latency budget. A few seconds of total retry time is usually sufficient.

### Cache-bust the CDN if needed

Some aggregators sit behind a CDN that caches 404 responses. If retries keep returning a cached 404, append a cache-busting query parameter:

```ts
fetch(`${aggregatorUrl}/v1/blobs/${blobId}?cb=${Date.now()}`);
```

Note that some CDN configurations ignore unknown query parameters entirely, so this may not work for every aggregator. Once the blob is reliably reachable, drop the cache-buster.

### Use multiple aggregators

If a single aggregator is slow to surface the blob, falling back to another usually resolves it. Walrus is a distributed read network — if one aggregator returns 404 but another returns the blob, the blob is on the network.

### Pre-warm the read path before demos

Before showing a freshly uploaded blob to an audience, retrieve it once from the aggregator you plan to use. This forces the aggregator and any CDN in front of it to populate its cache before you need it.

## What this is not

- It is not a bug in Walrus.
- It does not mean the blob was lost. If you can read it from any aggregator, the blob is on the network.
- It is not specific to small or large blobs.