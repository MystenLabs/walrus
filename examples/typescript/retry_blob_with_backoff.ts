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
