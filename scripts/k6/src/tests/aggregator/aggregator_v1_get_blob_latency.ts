// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0
//
// Retrieve a fixed number of blobs from Walrus and report the metrics.
//
// The blob IDs to fetch are read from the Redis database located at `REDIS_URL`
// under the key with the same name as `PAYLOAD_SIZE`.
//
// For example,
// ```
// k6 run --env VUS=3 --env BLOBS_TO_READ=20 --env PAYLOAD_SIZE=10Mi \
//   --env ENVIRONMENT=localhost aggregator_v1_get_blob_latency.ts
// ```
//
// See `environment.ts` for ENVIRONMENT defaults.
import { DEFAULT_ENVIRONMENT, loadEnvironment } from '../../config/environment.ts'
import { getTestIdTags, loadParameters, parseHumanFileSize } from "../../lib/utils.ts"
import { BlobHistory } from "../../lib/blob_history.ts"
import { check } from 'k6';
import exec from 'k6/execution';
// @ts-ignore
import { randomIntBetween } from 'https://jslib.k6.io/k6-utils/1.2.0/index.js'
import { getBlob } from '../../flows/aggregator.ts';

interface TestParameters {
    /** Number of blobs to read. */
    blobsToRead: number,
    /** The number of virtual users that will repeatedly read blobs. */
    vus: number,
    /**
     * Used as the key into the redis key-value store to fetch blob IDs
     * corresponding to blobs of the given size.
     */
    payloadSize: string,
    /** The timeout for fetching each blob. */
    timeout: string,
    /** The environment in which the test is running. */
    environment: string,
}

const params = loadParameters<TestParameters>(
    {
        blobsToRead: 20,
        vus: 1,
        payloadSize: "1Ki",
        timeout: "1m",
        environment: DEFAULT_ENVIRONMENT,
    },
    "aggregator/v1_get_blob_latency.plans.json"
);

const env = loadEnvironment(params.environment);

const blobHistory = new BlobHistory(env.redisUrl);

export const options = {
    scenarios: {
        "getBlobs": {
            executor: 'shared-iterations',
            vus: `${params.vus}`,
            iterations: `${params.blobsToRead}`,
            maxDuration: "15m",
        }
    },

    // Skip TLS verification for self-signed certs.
    insecureSkipTLSVerify: true,

    tags: {
        ...getTestIdTags(),
        payload_size: `${params.payloadSize}`,
        payload_size_bytes: `${parseHumanFileSize(params.payloadSize)}`,
    },
};

export async function setup(): Promise<string[]> {
    const humanFileSize = parseHumanFileSize(params.payloadSize);

    console.log('');
    console.log(`Aggregator URL: ${env.aggregatorUrl}`);
    console.log(`Blobs to read: ${params.blobsToRead}`);
    console.log(`Virtual users: ${params.vus}`);
    console.log(`Payload size: ${params.payloadSize} (${humanFileSize} B)`);
    console.log(`Blob read timeout: ${params.timeout}`);

    if (!env.redisUrl) {
        exec.test.abort("REDIS_URL must be defined");
    }

    const blobIds = await blobHistory.list(params.payloadSize);
    const count = blobIds.length;
    console.log(`Blob IDs: source="${env.redisUrl}", key="${params.payloadSize}", count=${count}`);

    return blobIds
}

export default async function (blobIds: string[]) {
    const blobId = blobIds[randomIntBetween(0, blobIds.length - 1)];
    const response = await getBlob(env.aggregatorUrl, blobId, params.timeout);

    check(response, {
        'is status 200': (r) => r.status === 200,
    });
}
