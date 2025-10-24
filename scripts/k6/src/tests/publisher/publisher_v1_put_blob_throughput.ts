// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0
//
// Measures the throughput of storing files by increasing the write rate until failure.
//
// The script sends blob put requests at increasing arrival rates and stops when the a request
// failure threshold is crossed. The various arrival rates that are tested are defined by sequence
// `arrivalRate_i = startRatePerMinute + (i - 1) * rateIncrement`.
//
// For a given stage of the test, the script is either ramping up to the next tested rate, or it
// is holding the the arrival rate steady to check if the publisher can support that arrival rate.
//
import { putBlob } from "../../flows/publisher.ts"
import { loadEnvironment } from "../../config/environment.ts"
import { open } from 'k6/experimental/fs';
import {
    StageConfig, countSteadyStageFailures, createStageThresholds, createStages,
    getTargetRpmTags, loadParameters, logObject, parseHumanFileSize, recordEndTime, recordStartTime
} from "../../lib/utils.ts"
import { check } from "k6";
import { BlobHistory } from "../../lib/blob_history.ts"


/**
 * Test parameters read from the environment in UPPER_CASE with a WALRUS_K6_ prefix.
 */
interface TestParameters extends StageConfig {
    /** The payload size of each request with a Ki or Mi or Gi suffix. */
    payloadSize: string,
    /** The timeout for storing each blob. */
    timeout: string,
    /** The number of pre-allocated virtual users. */
    preallocatedVus: number,
}

/** Common environment setting like URLs, loaded from __ENV with defaults. */
const env = loadEnvironment();
/** File providing random bytes to be used for blob files. */
const dataFile = await open(env.payloadSourceFile);
/** Handle for storing the IDs of blobs written to the network. */
const blobHistory = new BlobHistory(env.redisUrl);
/** Parameters from __ENV with defaults. */
const params = loadParameters<TestParameters>({
    startRatePerMinute: 40,
    rateIncrement: 5,
    heldStageDuration: "30s",
    heldStageCount: 5,
    payloadSize: "1Ki",
    timeout: "5m",
    preallocatedVus: 50,
    rampStageDuration: "5s",
    requestSloMillis: 600000,
});
/** The stages with different arrival rates, used to setup the test and in metrics. */
const stages = createStages(params);

export const options = {
    scenarios: {
        "putBlobsRequestThroughput": {
            executor: 'ramping-arrival-rate',
            startRate: 0,
            timeUnit: '1m',
            preAllocatedVUs: params.preallocatedVus,
            stages: stages,
        }
    },
    thresholds: createStageThresholds(params, params.requestSloMillis),
    insecureSkipTLSVerify: true,
};

export function setup(): number {
    recordStartTime();
    logObject(params, env);
    return parseHumanFileSize(params.payloadSize)
}

export default async function (fileSizeBytes: number) {
    const response = await putBlob(
        dataFile,
        env.publisherUrl,
        fileSizeBytes,
        { timeout: params.timeout, tags: getTargetRpmTags(stages) }
    );

    const isFailure = response.status != 200 || response.timings.duration > params.requestSloMillis;
    countSteadyStageFailures(isFailure, stages)

    check(response, { 'is status 200': (r) => r.status === 200 });

    await blobHistory.maybeRecordFromResponse(`blob_ids:${params.payloadSize}`, response);
}

export function teardown() {
    recordEndTime();
}
