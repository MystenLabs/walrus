// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0
//
// Reads files of a given size from Walrus at an increasing the rate until the
// aggregator starts failing.
//
// This script fetches blobs from a Walrus aggreator starting at 1 blob per
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
//   --env ENVIRONMENT=localhost aggregator_v1_get_blob_breakpoint.ts
// ```
// ramps up reading files, each 50 KiB in size up to a target rate of
// 300 per minute, over 15 minutes, and using an aggregator already running on
// localhost.
//
// See `environment.ts` for ENVIRONMENT defaults.
import exec from 'k6/execution';
import { BlobHistory } from "../../lib/blob_history.ts"
import { REDIS_URL, AGGREGATOR_URL } from '../../config/environment.ts'
import { check } from 'k6';
import { parseHumanFileSize } from "../../lib/utils.ts"
import { getBlob } from '../../flows/aggregator.ts';
// @ts-ignore
import { randomIntBetween } from 'https://jslib.k6.io/k6-utils/1.2.0/index.js'

/**
 * Used as the key into the redis key-value store to fetch blob IDs
 * corresponding to blobs of the given size.
 * @default '1Ki'
 */
const PAYLOAD_SIZE: string = __ENV.PAYLOAD_SIZE || '1Ki';

/**
 * The timeout for fetching each blob.
 * @default 5m
 */
const TIMEOUT: string = __ENV.TIMEOUT || '5m';

/**
 * The target arrival rate to ramp-up to in requests per minute.
 * @default 600
 */
const TARGET_RATE: number = parseInt(__ENV.TARGET_RATE || '600')

/**
 * The number of pre-allocated virtual users.
 * @default 500
 */
const PREALLOCATED_VUS: number = parseInt(__ENV.PREALLOCATED_VUS || '500');

/**
 * The duration over which to ramp-up the requests.
 *
 * This determines how quickly the arrival rate increases, and the test likely ends
 * long before actually reaching this duration.
 * @default 30m
 */
const DURATION: string = __ENV.DURATION || '30m';


export const options = {
    scenarios: {
        "getBlobsBreakpoint": {
            executor: 'ramping-arrival-rate',
            startRate: 1,
            timeUnit: '1m',
            preAllocatedVUs: PREALLOCATED_VUS,
            stages: [
                { target: TARGET_RATE, duration: DURATION },
            ]
        }
    },

    thresholds: {
        http_req_failed: [{ threshold: 'rate <= 0.05', abortOnFail: true }],
        checks: [{ threshold: 'rate >= 0.95', abortOnFail: true }],
    },

    // Skip TLS verification for self-signed certs.
    insecureSkipTLSVerify: true,
};

const blobHistory = new BlobHistory(REDIS_URL);

export async function setup(): Promise<{ blobIdCount: number, baseDurationMillis: number }> {
    console.log('');
    console.log(`Aggregator URL: ${AGGREGATOR_URL}`);
    console.log(`Target rate: ${TARGET_RATE} req/min`);
    console.log(`Ramp-up duration: ${DURATION}`);
    console.log(`Payload size: ${PAYLOAD_SIZE} (${parseHumanFileSize(PAYLOAD_SIZE)} B)`);
    console.log(`Blob read timeout: ${TIMEOUT}`);

    if (REDIS_URL == undefined) {
        exec.test.abort("REDIS_URL must be defined");
    }
    const count = await blobHistory.len(PAYLOAD_SIZE);
    console.log(`Blob IDs: source="${REDIS_URL}", key="${PAYLOAD_SIZE}", count=${count}`);

    // Sample the duration to get a blob.
    const sampleBlobId = await blobHistory.blobIdAtIndex(PAYLOAD_SIZE, 0);
    const result = await getBlob(AGGREGATOR_URL, sampleBlobId!, TIMEOUT);
    console.log(`Baseline GET duration: ${result.timings.duration}ms`);

    return { blobIdCount: count!, baseDurationMillis: result.timings.duration }
}

export default async function (state: { blobIdCount: number, baseDurationMillis: number }) {
    const blobIdIndex = randomIntBetween(0, state.blobIdCount - 1);
    const blobId = await blobHistory.blobIdAtIndex(PAYLOAD_SIZE, blobIdIndex);

    const response = await getBlob(AGGREGATOR_URL, blobId!, TIMEOUT);

    check(response, {
        'is status 200': (r) => r.status === 200,
    });
    check(response, {
        'duration is at most twice baseline': (r) => {
            return r.timings.duration <= 2 * state.baseDurationMillis
        }
    });
}
