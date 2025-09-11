// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0
//
// Store files of a given size on Walrus at an increasing the rate until the
// publisher starts failing.
//
// This script stores blobs of the size specified by `PAYLOAD_SIZE` with the
// publisher starting at 1 blob per minute. It increases this rate up to
// `TARGET_RATE` over the specified duration `DURATION`
//
// It stops when either (i) more than 5% of the requests fail, which happens
// when the publisher refuses to accept new requests, or (ii) the time to store
// a file is more than twice what it was as the beginning of the test.
//
// The resulting metrics can be used to determine the supported throughput of
// the publisher in terms of requests/minute (use small files), and bytes
// per minute (use large files).
//
// For example,
// ```
// k6 run --env PAYLOAD_SIZE=50Ki --env DURATION=15m --env TARGET_RATE=300 \
//   --env ENVIRONMENT=localhost publisher_v1_put_blobs_breakpoint.ts
// ```
// ramps up storing files, each 50 KiB in size up to a target rate of
// 300 per minute, over 15 minutes, and using a publisher already running on
// localhost.
//
// See `environment.ts` for ENVIRONMENT defaults.
import { PutBlobOptions, putBlob } from "../../flows/publisher.ts"
import { PUBLISHER_URL, PAYLOAD_SOURCE_FILE } from "../../config/environment.ts"
import { open } from 'k6/experimental/fs';
import { parseHumanFileSize } from "../../lib/utils.ts"
import { check } from "k6";

/**
 * The number of pre-allocated virtual users.
 * @default 500
 */
const PREALLOCATED_VUS: number = parseInt(__ENV.PREALLOCATED_VUS || '500');

/**
 * The payload size of each request with a Ki or Mi or Gi suffix.
 * @default 1Ki
 */
const PAYLOAD_SIZE: string = __ENV.PAYLOAD_SIZE || '1Ki';

/**
 * The timeout for storing each blob.
 * @default 5m
 */
const TIMEOUT: string = __ENV.TIMEOUT || '5m';

/**
 * The target arrival rate to ramp-up to in requests per minute.
 * @default 600
 */
const TARGET_RATE: number = parseInt(__ENV.TARGET_RATE || '600')

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
        "putBlobsBreakpoint": {
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
    tags: {
        "payload-size": `${PAYLOAD_SIZE}`,
    },

    // Skip TLS verification for self-signed certs.
    insecureSkipTLSVerify: true,
};

const dataFile = await open(PAYLOAD_SOURCE_FILE);
const payloadSize = parseHumanFileSize(PAYLOAD_SIZE);

export async function setup(): Promise<number> {
    console.log('');
    console.log(`Publisher URL: ${PUBLISHER_URL}`);
    console.log(`Data file path: ${PAYLOAD_SOURCE_FILE}`);
    console.log(`Payload size: ${PAYLOAD_SIZE}`);
    console.log(`Blob store timeout: ${TIMEOUT}`);
    console.log(`Target rate: ${TARGET_RATE} req/min`);
    console.log(`Ramp-up duration: ${DURATION}`);

    // Sample the duration to put a blob.
    const result = await putBlob(
        dataFile, PUBLISHER_URL, new PutBlobOptions(payloadSize, payloadSize, TIMEOUT)
    )
    console.log(`Baseline PUT duration: ${result.timings.duration}ms`);
    return result.timings.duration;
}

/**
 * Run the test and checks that the duration is at most twice the base duration.
 */
export default async function (basePutDurationMillis: number) {
    const payloadSize = parseHumanFileSize(PAYLOAD_SIZE);
    const result = await putBlob(
        dataFile, PUBLISHER_URL, new PutBlobOptions(payloadSize, payloadSize, TIMEOUT)
    );
    check(result, {
        'duration is at most twice baseline': (r) => r.timings.duration <= 2 * basePutDurationMillis
    });
}
