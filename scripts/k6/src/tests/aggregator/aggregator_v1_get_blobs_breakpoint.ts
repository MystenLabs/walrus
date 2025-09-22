// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0
//
// Reads files of a given size from Walrus at an increasing the rate until the
// aggregator starts failing.
//
// This script fetches blobs from a Walrus aggregator starting at 1 blob per
// minute. It increases this rate up to `TARGET_RATE` over the specified
// duration `DURATION`.
//
// The blob IDs to fetch are read from the Redis database located at `REDIS_URL`
// under the key with the same name as `PAYLOAD_SIZE`.
//
// It stops when either (i) more than 5% of the requests fail or (ii) the time
// to read a file is more than twice what it was as the beginning of the test,
// in more than 5% of the cases.
//
// The resulting metrics can be used to determine the supported throughput of
// the aggregator in terms of requests/minute (use small files), and bytes
// per minute (use large files).
//
// For example,
// ```
// k6 run --env PAYLOAD_SIZE=50Ki --env DURATION=15m --env TARGET_RATE=300 \
//   --env ENVIRONMENT=localhost aggregator_v1_get_blobs_breakpoint.ts
// ```
// ramps up reading files, each 50 KiB in size up to a target rate of
// 300 per minute, over 15 minutes, and using an aggregator already running on
// localhost.
//
// See `environment.ts` for ENVIRONMENT defaults.
import exec from 'k6/execution';
import { BlobHistory } from "../../lib/blob_history.ts"
import { DEFAULT_ENVIRONMENT, loadEnvironment } from '../../config/environment.ts'
import { check } from 'k6';
import { getTestIdTags, loadParameters, parseHumanFileSize } from "../../lib/utils.ts"
import { getBlob } from '../../flows/aggregator.ts';
// @ts-ignore
import { randomIntBetween } from 'https://jslib.k6.io/k6-utils/1.2.0/index.js'


interface TestParameters {
    /** The number of pre-allocated virtual users. */
    preallocatedVus: number,
    /**
     * Used as the key into the redis key-value store to fetch blob IDs
     * corresponding to blobs of the given size.
     */
    payloadSize: string,
    /** The timeout for reading each blob. */
    timeout: string
    /** The target arrival rate to ramp-up to in requests per minute **/
    targetRate: number,
    /**
     * The duration over which to ramp-up the requests.
     *
     * This determines how quickly the arrival rate increases, and the test likely ends
     * long before actually reaching this duration.
     */
    duration: string
    /** The environment in which the test is running. */
    environment: string,
}

const params = loadParameters<TestParameters>(
    {
        preallocatedVus: 500,
        payloadSize: "1Ki",
        timeout: "1m",
        targetRate: 600,
        duration: "30m",
        environment: DEFAULT_ENVIRONMENT,
    },
    "aggregator/v1_get_blobs_breakpoint.plans.json"
);

const env = loadEnvironment(params.environment);

const blobHistory = new BlobHistory(env.redisUrl);

export const options = {
    scenarios: {
        "getBlobsBreakpoint": {
            executor: 'ramping-arrival-rate',
            startRate: 1,
            timeUnit: '1m',
            preAllocatedVUs: `${params.preallocatedVus}`,
            stages: [
                { target: `${params.targetRate}`, duration: `${params.duration}` },
            ]
        }
    },

    thresholds: {
        http_req_failed: [{ threshold: 'rate <= 0.05', abortOnFail: true }],
        checks: [{ threshold: 'rate >= 0.95', abortOnFail: true }],
    },

    // Skip TLS verification for self-signed certs.
    insecureSkipTLSVerify: true,

    tags: {
        ...getTestIdTags(),
        payload_size: `${params.payloadSize}`,
    },
};

export async function setup(): Promise<{ blobIdCount: number, baseDurationMillis: number }> {
    const humanFileSize = parseHumanFileSize(params.payloadSize);

    console.log('');
    console.log(`Aggregator URL: ${env.aggregatorUrl}`);
    console.log(`Target rate: ${params.targetRate} req/min`);
    console.log(`Ramp-up duration: ${params.duration}`);
    console.log(`Payload size: ${params.payloadSize} (${humanFileSize} B)`);
    console.log(`Blob read timeout: ${params.timeout}`);

    if (!env.redisUrl) {
        exec.test.abort("REDIS_URL must be defined");
    }
    const count = await blobHistory.len(params.payloadSize);
    console.log(`Blob IDs: source="${env.redisUrl}", key="${params.payloadSize}", count=${count}`);

    // Sample the duration to get a blob.
    const sampleBlobId = await blobHistory.blobIdAtIndex(params.payloadSize, 0);
    const result = await getBlob(env.aggregatorUrl, sampleBlobId!, params.timeout);
    console.log(`Baseline GET duration: ${result.timings.duration}ms`);

    return { blobIdCount: count!, baseDurationMillis: result.timings.duration }
}

export default async function (state: { blobIdCount: number, baseDurationMillis: number }) {
    const blobIdIndex = randomIntBetween(0, state.blobIdCount - 1);
    const blobId = await blobHistory.blobIdAtIndex(params.payloadSize, blobIdIndex);

    const response = await getBlob(env.aggregatorUrl, blobId!, params.timeout);

    check(response, {
        'is status 200': (r) => r.status === 200,
    });
    check(response, {
        'duration is at most twice baseline': (r) => {
            return r.timings.duration <= 2 * state.baseDurationMillis
        }
    });
}
