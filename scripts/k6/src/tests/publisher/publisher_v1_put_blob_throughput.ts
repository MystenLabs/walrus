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
// The prometheus metric `k6_stage_failed_rate` reports the number of requests at a given target
// rate that failed, and can be used to find the highest supported request rate.
//
import { putBlob } from "../../flows/publisher.ts"
import { loadEnvironment } from "../../config/environment.ts"
import { open } from 'k6/experimental/fs';
import { loadParameters, parseHumanFileSize } from "../../lib/utils.ts"
import { check } from "k6";
import { BlobHistory } from "../../lib/blob_history.ts"
// @ts-ignore
import { getCurrentStageIndex, tagWithCurrentStageIndex, tagWithCurrentStageProfile } from 'https://jslib.k6.io/k6-utils/1.6.0/index.js'
import { Rate } from "k6/metrics";


/**
 * Test parameters read from the environment in UPPER_CASE with a WALRUS_K6_ prefix.
 */
interface TestParameters {
    /** The start rate in requests per minute. */
    startRatePerMinute: number,
    /** The amount to increment the rate by at each stage. */
    rateIncrement: number,
    /** The duration of each held (non-ramping) stage. */
    heldStageDuration: string,
    /** The duration of each ramping stage. */
    rampStageDuration: string,
    /** The number of stages where the arrival rate is held constant. */
    heldStageCount: number
    /** The payload size of each request with a Ki or Mi or Gi suffix. */
    payloadSize: string,
    /** The timeout for storing each blob. */
    timeout: string,
    /** The number of pre-allocated virtual users. */
    preallocatedVus: number,
}

const env = loadEnvironment();
const params = loadParameters<TestParameters>({
    startRatePerMinute: 40,
    rateIncrement: 5,
    heldStageDuration: "30s",
    heldStageCount: 10,
    payloadSize: "1Ki",
    timeout: "5m",
    preallocatedVus: 50,
    rampStageDuration: "5s"
});
const dataFile = await open(env.payloadSourceFile);
const blobHistory = new BlobHistory(env.redisUrl);
const stages = createStages(params);
const stageFailureRate = new Rate('stage_failed');

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
    thresholds: {
        http_req_failed: [{ threshold: 'rate <= 0.05', abortOnFail: true }],
    },
    insecureSkipTLSVerify: true,
};


export function setup(): number {
    const fileSizeBytes = parseHumanFileSize(params.payloadSize);

    console.log('');
    console.log(`Publisher URL: ${env.publisherUrl}`);
    console.log(`Data file path: ${env.payloadSourceFile}`);
    console.log(`Payload size: ${params.payloadSize} (${fileSizeBytes} B)`);
    console.log(`Blob store timeout: ${params.timeout}`);
    console.log(`Arrival rate progression i, in [0, ${params.heldStageCount}): ` +
        `${params.startRatePerMinute} + ${params.rateIncrement}*i`);
    console.log(`Arrival rate hold duration: ${params.heldStageDuration}`);

    if (env.redisUrl != undefined) {
        console.log(`Blob history written to: ${env.redisUrl}`);
    }

    return fileSizeBytes
}

export default async function (fileSizeBytes: number) {
    const response = await putBlob(
        dataFile, env.publisherUrl, fileSizeBytes, { timeout: params.timeout }
    );

    countSteadyStageFailures(response.status == 200)

    await blobHistory.maybeRecordFromResponse(params.payloadSize, response);

    check(response, {
        'is status 200': (r) => r.status === 200,
    });
}

function createStages(params: TestParameters): { duration: string, target: number }[] {
    const stages = [];
    var target = params.startRatePerMinute;
    for (var i = 0; i < params.heldStageCount; ++i) {
        stages.push({ target: target, duration: params.rampStageDuration });
        stages.push({ target: target, duration: params.heldStageDuration });
        target += params.rateIncrement;
    }
    return stages;
}

function countSteadyStageFailures(is_success: boolean) {
    const stageIndex = getCurrentStageIndex();
    const isSteadyStage = (stageIndex % 2 == 1);

    if (isSteadyStage) {
        stageFailureRate.add(
            !is_success,
            {
                target_rpm: stages[stageIndex].target.toString(),
                stage_profile: (stageIndex % 2 == 0) ? 'ramp-up' : 'steady'
            }
        );
    }
}
