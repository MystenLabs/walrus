// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0
//
// Retrieve blobs from an aggregator at an increasing rate, ending when requests
// begin to fail or the request service-level-objective SLO is exceeded.
//
// The script requests blobs at increasing arrival rates and stops when the request failure
// threshold is crossed. The various arrival rates that are tested are defined by sequence
// `arrivalRate_i = startRatePerMinute + (i - 1) * rateIncrement`.
//
// For a given stage of the test, the script is either ramping up to the next tested rate, or it
// is holding the the arrival rate steady to check if the aggregator can support that arrival rate.
//
// The blob IDs to fetch are read from the Redis database located at `WALRUS_K6_REDIS_URL`
// under the key with the name `blob_ids:<WALRUS_K6_PAYLOAD_SIZE>`.
//
import { BlobHistory } from "../../lib/blob_history.ts"
import { loadEnvironment } from '../../config/environment.ts'
import { check } from 'k6';
import {
    StageConfig,  createStageThresholds, createStages, ensure, getTargetRpmTags, loadParameters,
    logObject, countSteadyStageFailures, recordEndTime, recordStartTime
} from "../../lib/utils.ts"
import { getBlob } from '../../flows/aggregator.ts';
import { randomIntBetween }
    // @ts-ignore
    from 'https://jslib.k6.io/k6-utils/1.6.0/index.js'


/**
 * Test parameters read from the environment in UPPER_CASE with a WALRUS_K6_ prefix.
 */
interface TestParameters extends StageConfig {
    /** The payload size of the blobs being read, with a Ki, Mi or Gi suffix. */
    payloadSize: string,
    /** The timeout for reading each blob. */
    timeout: string,
    /** The number of pre-allocated virtual users. */
    preallocatedVus: number,
}

/** Common environment setting like URLs, loaded from __ENV with defaults. */
const env = loadEnvironment();
/** Handle for storing the IDs of blobs written to the network. */
const blobHistory = new BlobHistory(env.redisUrl);
/** Parameters from __ENV with defaults. */
const params = loadParameters<TestParameters>({
    preallocatedVus: 500,
    payloadSize: "1Ki",
    startRatePerMinute: 100,
    rateIncrement: 10,
    heldStageDuration: "10s",
    heldStageCount: 5,
    rampStageDuration: "5s",
    timeout: "3m",
    requestSloMillis: 600000,
});
/** Key used to lookup blob IDs from the redis database. */
const blobIdsKey = `blob_ids:${params.payloadSize}`;
/** The stages with different arrival rates, used to setup the test and in metrics. */
const stages = createStages(params);


export const options = {
    scenarios: {
        "getBlobsBreakpoint": {
            executor: 'ramping-arrival-rate',
            startRate: 0,
            timeUnit: '1m',
            preAllocatedVUs: `${params.preallocatedVus}`,
            stages: stages,
        }
    },
    thresholds: createStageThresholds(params, params.requestSloMillis),
    insecureSkipTLSVerify: true,
};

export async function setup(): Promise<number> {
    recordStartTime();
    logObject(params, env);

    ensure(env.redisUrl !== undefined, "WALRUS_K6_REDIS_URL must be defined");
    const blobIdCount = await blobHistory.len(blobIdsKey);
    ensure(
        (blobIdCount !== null && blobIdCount >= 10),
        `insufficient blob IDs stored under ${blobIdsKey}: ${blobIdCount}`
    );

    return blobIdCount!;
}

export default async function (blobIdCount: number) {
    const blobIdIndex = randomIntBetween(0, blobIdCount - 1);
    const blobId = await blobHistory.index(blobIdsKey, blobIdIndex);

    const response = await getBlob(
        env.aggregatorUrl, blobId!, { timeout: params.timeout, tags: getTargetRpmTags(stages) }
    );

    const isFailure = response.status != 200 || response.timings.duration > params.requestSloMillis;
    countSteadyStageFailures(isFailure, stages)

    check(response, { 'is status 200': (r) => r.status === 200 });
}

export function teardown() {
    recordEndTime();
}
