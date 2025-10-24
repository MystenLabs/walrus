// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0
//
// Retrieve a fixed number of blobs from Walrus and report the metrics.
//
// The blob IDs to fetch are read from the Redis database located at `WALRUS_K6_REDIS_URL`
// under the key with the name `blob_ids:<WALRUS_K6_PAYLOAD_SIZE>`.
//
import { randomIntBetween }
    // @ts-ignore
    from 'https://jslib.k6.io/k6-utils/1.6.0/index.js'
import { expect }
    // @ts-ignore
    from 'https://jslib.k6.io/k6-testing/0.5.0/index.js';
import { loadEnvironment } from '../../config/environment.ts'
import { ensure, loadParameters, logObject, recordEndTime, recordStartTime }
    from "../../lib/utils.ts"
import { BlobHistory } from "../../lib/blob_history.ts"
import { getBlob } from '../../flows/aggregator.ts';

/**
 * Test parameters read from the environment in UPPER_CASE with a WALRUS_K6_ prefix.
 */
interface TestParameters {
    /** Number of blobs to read. */
    blobCount: number,
    /** The number of virtual users that will repeatedly read blobs. */
    maxConcurrency: number,
    /**
     * Used as the key into the redis key-value store to fetch blob IDs
     * corresponding to blobs of the given size.
     */
    payloadSize: string,
    /** Value in milliseconds that will be used to set a threshold on http request duration */
    httpDurationThreshold: string,
    /** The timeout for fetching each blob. */
    timeout: string,
}

/** Common environment setting like URLs, loaded from __ENV with defaults. */
const env = loadEnvironment();
/** Handle for storing the IDs of blobs written to the network. */
const blobHistory = new BlobHistory(env.redisUrl);
/** Parameters from __ENV with defaults. */
const params = loadParameters<TestParameters>({
    blobCount: 20,
    maxConcurrency: 1,
    payloadSize: "1Ki",
    httpDurationThreshold: "",
    timeout: "10m",
});
/** Key used to lookup blob IDs from the redis database. */
const blobIdsKey = `blob_ids:${params.payloadSize}`;

export const options = {
    scenarios: {
        "getBlobs": {
            executor: 'shared-iterations',
            vus: `${params.maxConcurrency}`,
            iterations: `${params.blobCount}`,
            maxDuration: "15m",
        }
    },
    thresholds: !params.httpDurationThreshold ? {} : {
        http_req_duration: [`p(99) < ${params.httpDurationThreshold}`]
    },
    insecureSkipTLSVerify: true,
};

export async function setup(): Promise<number> {
    recordStartTime();
    logObject(params, env);

    ensure(env.redisUrl !== undefined, "WALRUS_K6_REDIS_URL must be defined");
    const blobIdCount = await blobHistory.len(blobIdsKey);
    ensure(
        (blobIdCount !== null && blobIdCount >= params.blobCount),
        `insufficient blob IDs stored under ${blobIdsKey}: ${blobIdCount}`
    );

    return blobIdCount!;
}

export default async function (blobIdCount: number) {
    const blobIdIndex = randomIntBetween(0, blobIdCount - 1);
    const blobId = await blobHistory.index(blobIdsKey, blobIdIndex);
    const response = await getBlob(env.aggregatorUrl, blobId!, { timeout: params.timeout });

    expect(response.status).toBe(200);
}

export function teardown() {
    recordEndTime();
}
