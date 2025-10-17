// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0
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
import { getQuiltPatch } from '../../flows/aggregator.ts';

/**
 * Test parameters read from the environment in UPPER_CASE with a WALRUS_K6_ prefix.
 */
interface TestParameters {
    /** Number of blobs to read. */
    patchesToRead: number,
    /** The number of virtual users that will repeatedly read blobs. */
    maxConcurrency: number,
    /** The key into the Redis instance under which patches IDs. */
    patchIdListKey: string,
    /** The timeout for fetching each blob. */
    timeout: string,
}

/** Common environment setting like URLs, loaded from __ENV with defaults. */
const env = loadEnvironment();
/** Handle for storing the IDs of blobs written to the network. */
const blobHistory = new BlobHistory(env.redisUrl);
/** Parameters from __ENV with defaults. */
const params = loadParameters<TestParameters>({
    patchesToRead: 20,
    maxConcurrency: 1,
    patchIdListKey: 'quilt:1Mi:uniform:patches',
    timeout: "1m",
});

export const options = {
    scenarios: {
        "getBlobs": {
            executor: 'shared-iterations',
            vus: `${params.maxConcurrency}`,
            iterations: `${params.patchesToRead}`,
            maxDuration: "15m",
        }
    },
    insecureSkipTLSVerify: true,
};

export async function setup(): Promise<number> {
    recordStartTime();
    logObject(params, env);

    ensure(env.redisUrl !== undefined, "WALRUS_K6_REDIS_URL must be defined");
    const patchCount = await blobHistory.len(params.patchIdListKey);
    ensure(
        (patchCount !== null && patchCount >= params.patchesToRead),
        `insufficient patches stored under ${params.patchIdListKey}: ${patchCount}`
    );

    return patchCount!;
}

export default async function (patchIdCount: number) {
    const patchIndex = randomIntBetween(0, patchIdCount - 1);
    const quiltOrPatchId = await blobHistory.index(params.patchIdListKey, patchIndex);
    const response = await getQuiltPatch(env.aggregatorUrl, quiltOrPatchId!, params.timeout);

    expect(response.status).toBe(200);
}

export function teardown() {
    recordEndTime();
}
