> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

Walrus itself is strongly consistent. Once a blob is certified, any aggregator reading directly from storage nodes returns it immediately. However, public aggregators are commonly fronted by a content delivery network (CDN), and CDNs may cache a `404 Not Found` response from a brief moment when the aggregator had not yet seen the blob. This is what causes the apparent "blob is missing right after upload" symptom.

If your app reads blobs through a cached aggregator immediately after upload, plan for this window.

## When this applies

This propagation window only affects you in specific conditions:

- You are reading through a public aggregator with a CDN in front. Examples include the Mainnet and Testnet public aggregators.
- You read a blob within seconds of its certification.

It does not apply when:

- You are reading from a self-hosted aggregator with no CDN in front. Reads are strongly consistent.
- The blob does not exist at all. A `404` in that case is correct and should not be retried.

## Retry only when you know the blob should exist

Because Walrus is strongly consistent, a `404` from an uncached aggregator means the blob genuinely is not on the network. If you blindly retry every `404`, you inflate read latency for the common case of "this blob does not exist."

Retry with backoff only when your app knows the blob has just been certified. Treat other `404` responses as terminal.

## Example: retry after a known upload

The helper below is meant for the post-upload path: your app just received a successful certification, then immediately wants to fetch the blob through a cached aggregator. It surfaces non-404 errors immediately and only retries `404`.

```ts title="examples/typescript/retry_blob_with_backoff.ts"
// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

/**
  * Fetch a blob immediately after upload from a CDN-fronted aggregator.
 *
  * Walrus is strongly consistent, but CDNs in front of public aggregators might
  * briefly cache a 404 from before the blob was visible. Use this helper only
  * when your app knows the blob has just been certified. For general reads,
  * treat 404 as terminal instead of retrying.
 */
async function fetchBlobWithRetry(
  blobId: string,
  aggregatorUrl: string,
  opts = { maxAttempts: 6, baseDelayMs: 500 },
): Promise<ArrayBuffer> {
  let lastError: unknown;
  for (let attempt = 0; attempt < opts.maxAttempts; attempt++) {
    try {
      const res = await fetch(`${aggregatorUrl}/v1/blobs/${blobId}`, { cache: "no-store" });
      if (res.ok) return await res.arrayBuffer();
      if (res.status !== 404) throw new Error(`Unexpected status ${res.status}`);
    } catch (err) {
      lastError = err;
    }
    const delay = opts.baseDelayMs * 2 ** attempt;
    await new Promise((r) => setTimeout(r, delay));
  }
  throw lastError ?? new Error(`Blob ${blobId} not available after ${opts.maxAttempts} attempts`);
}
```

Tune `maxAttempts` and `baseDelayMs` to your latency budget. A few seconds of total retry time is usually sufficient. Do not use this pattern for general reads. For those, treat `404` as terminal.

#### Cache-bust the CDN if needed

Some CDNs may not respect cache control headers and can cache the `404` response. If retries keep hitting the cached `404`, append a cache-busting query parameter:

```ts
fetch(`${aggregatorUrl}/v1/blobs/${blobId}?cb=${Date.now()}`);
```

Once the blob is reliably reachable, you can drop the cache-buster.

#### Use multiple aggregators

If a single aggregator is slow to surface the blob, falling back to another usually resolves it. If one aggregator returns 404 but another returns the blob, the blob is on the network.

#### Pre-warm the read path before demos

Before showing a freshly uploaded blob to an audience, retrieve it once from the aggregator you plan to use. This forces the aggregator and any CDN in front of it to populate its cache before you need it.