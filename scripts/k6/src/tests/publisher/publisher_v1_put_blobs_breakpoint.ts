// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0
//
// Store files of a given size on Walrus at an increasing the rate until the
// publisher starts failing.
//
// This script stores blobs of the size specified by `PAYLOAD_SIZE` with the
// publisher starting at 1 blob per minute. It increases this rate up to
// `TARGET_RATE` over the specified duration `DURATION`.
//
// It stops when either (i) more than 5% of the requests fail, which happens
// when the publisher refuses to accept new requests, or (ii) the time to store
// a file is more than twice what it was as the beginning of the test, in more
// than 5% of the cases.
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
import { DEFAULT_ENVIRONMENT, loadEnvironment } from "../../config/environment.ts"
import { open } from 'k6/experimental/fs';
import { loadParameters, parseHumanFileSize } from "../../lib/utils.ts"
import { check } from "k6";
import { BlobHistory } from "../../lib/blob_history.ts"

interface TestParameters {
    /** The number of pre-allocated virtual users. */
    preallocatedVus: number,
    /** The payload size of each request with a Ki or Mi or Gi suffix. */
    payloadSize: string,
    /** The timeout for storing each blob. */
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
    /** An identifier for the test run. */
    testId: string,
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
        testId: "",
        environment: DEFAULT_ENVIRONMENT,
    },
    "publisher/v1_put_blobs_breakpoint.plans.json"
);

const env = loadEnvironment(params.environment);

const dataFile = await open(env.payloadSourceFile);

const blobHistory = new BlobHistory(env.redisUrl);

const payloadSize = parseHumanFileSize(params.payloadSize);

export const options = {
    scenarios: {
        "putBlobsBreakpoint": {
            executor: 'ramping-arrival-rate',
            startRate: 1,
            timeUnit: '1m',
            preAllocatedVUs: params.preallocatedVus,
            stages: [
                { target: params.targetRate, duration: params.duration },
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
        "testid": `${params.testId}`,
        "payload_size": `${params.payloadSize}`,
    },
};


export async function setup(): Promise<number> {
    console.log('');
    console.log(`Publisher URL: ${env.publisherUrl}`);
    console.log(`Data file path: ${env.payloadSourceFile}`);
    console.log(`Payload size: ${params.payloadSize}`);
    console.log(`Blob store timeout: ${params.timeout}`);
    console.log(`Target rate: ${params.targetRate} req/min`);
    console.log(`Ramp-up duration: ${params.duration}`);
    if (env.redisUrl != undefined) {
        console.log(`Blob history written to: ${env.redisUrl}`);
    }

    // Sample the duration to put a blob.
    const result = await putBlob(
        dataFile, env.publisherUrl, new PutBlobOptions(payloadSize, payloadSize, params.timeout)
    )
    console.log(`Baseline PUT duration: ${result.timings.duration}ms`);
    return result.timings.duration;
}

/**
 * Run the test and checks that the duration is at most twice the base duration.
 */
export default async function (basePutDurationMillis: number) {
    const response = await putBlob(
        dataFile, env.publisherUrl, new PutBlobOptions(payloadSize, payloadSize, params.timeout)
    );

    await blobHistory.maybeRecordFromResponse(params.payloadSize, response);

    check(response, {
        'is status 200': (r) => r.status === 200,
    });
    check(response, {
        'duration is at most twice baseline': (r) => r.timings.duration <= 2 * basePutDurationMillis
    });
}

export async function teardown() {
    const blobIdCount = await blobHistory.len(params.payloadSize);
    if (blobIdCount != null) {
        console.log(`Total Blob IDs stored under key "${params.payloadSize}": ${blobIdCount}`);
    }
}
