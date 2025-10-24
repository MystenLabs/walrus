// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

import { randomIntBetween, getCurrentStageIndex }
    // @ts-ignore
    from 'https://jslib.k6.io/k6-utils/1.6.0/index.js'
import { expect }
    // @ts-ignore
    from 'https://jslib.k6.io/k6-testing/0.5.0/index.js';
import { File, SeekMode } from 'k6/experimental/fs';
import { randomBytes } from 'k6/crypto';
import { Gauge, Rate } from 'k6/metrics';

/**
 * Returns a random range `[start, length]` on the interval `[0, limit)`.
 * @param limit - The maximum value for `start + length`, denoting the highest possible
 * bound of the range.
 * @param minLength  - The minimum length of the range.
 * @param maxLength  - The maximum length of the range.
 * @returns - The tuple of start index and length of the range.
 */
export function randomRange(minLength: number, maxLength: number, limit: number): [number, number] {
    expect(maxLength).toBeLessThanOrEqual(limit);

    const start = randomIntBetween(0, limit - minLength);
    const length = randomIntBetween(minLength, Math.min(limit - start, maxLength));
    return [start, length];
}

/**
 * Asynchronously reads and returns a byte range from the file.
 * @param dataFile - The data file from which to read.
 * @param start - The start of the range to read.
 * @param length - The length of the range to read.
 * @throws Throws an error if if the indicated range is beyond the file length.
 * @returns The read data.
 */
export async function readFileRange(
    dataFile: File,
    start: number,
    length: number
): Promise<Uint8Array> {
    await dataFile.seek(start, SeekMode.Start);

    const buffer = new Uint8Array(length);
    const bytesRead = await dataFile.read(buffer);

    if (bytesRead != length) {
        throw new Error(`range ${start}:${length} pointed beyond file end`);
    }
    return buffer;
}


export function parseHumanFileSize(value: string): number {
    // parseInt just takes the numeric prefix and parses it.
    var numericValue = parseFloat(value);

    if (value.endsWith('Gi')) {
        return Math.floor(numericValue * 1024 * 1024 * 1024);
    }
    if (value.endsWith('Mi')) {
        return Math.floor(numericValue * 1024 * 1024);
    }
    if (value.endsWith('Ki')) {
        return Math.floor(numericValue * 1024);
    }
    return Math.floor(numericValue);
}

type Parameters = Record<string, string | number>;

/**
 * For each camelCase property in `example`, load the corresponding WALRUS_K6_<UPPER_SNAKE_CASE>
 * key from __ENV and return it.
 */
function loadEnv(keysAndTypes: Record<string, string>): Parameters {
    const output: Parameters = {}

    for (const [key, keyType] of Object.entries(keysAndTypes)) {
        const upperKey = "WALRUS_K6_" + key.replace(/([a-z0-9])([A-Z])/g, "$1_$2").toUpperCase();
        const value = __ENV[upperKey];
        if (value !== undefined) {
            if (keyType == "number") {
                output[key] = parseFloat(value)
            } else {
                output[key] = value;
            }
        }
    }
    return output
}


/**
 * Loads parameters from __ENV.
 *
 * @param defaults - The default parameters. The keys in the `defaults` are converted from camelCase
 * to UPPER_SNAKE_CASE, prepended with `WALRUS_K6_`, and fetched from the environment variables.
 * @returns The loaded parameters.
 */
export function loadParameters<T extends object>(defaults: T): T {
    var output = defaults;
    const keysAndTypes = Object.fromEntries(
        Object.entries(defaults).map(([key, value]) => [key, typeof (value)])
    );
    return Object.assign(output, loadEnv(keysAndTypes))
}


/**
 * Perturbs a random number (maximum 30) bytes in the provided data array.
 */
export function perturbData(array: Uint8Array): Uint8Array {
    const count = randomIntBetween(1, Math.min(array.length, 30))
    const newRandomBytes = randomBytes(count);

    for (var i = 0; i < newRandomBytes.byteLength; ++i) {
        const index = randomIntBetween(0, array.length - 1);
        array[index] = randomIntBetween(0, 255);
    }

    return array;
}

/**
 * Loads a random range of data from the file of length between the specified
 * minimum and maximum.
 */
export async function loadRandomData(
    dataFile: File, minLength: number, maxLength: number = minLength
): Promise<Uint8Array> {
    const dataFileLength = (await dataFile.stat()).size;
    const [dataStart, dataLength] = randomRange(minLength, maxLength, dataFileLength);
    return perturbData(await readFileRange(dataFile, dataStart, dataLength));
}

/**
 * Throws an error with the specified message if the condition is not true.
 */
export function ensure(condition: boolean, message: string) {
    if (!condition) {
        throw new Error(message);
    }
}


export interface Stage { duration: string, target: number }

export interface StageConfig {
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
    /**Objective for the request duration. Anything higher is considered a failed for that stage.*/
    requestSloMillis: number,
}

const stageFailureRate = new Rate('throughput_stage_failed');

export function countSteadyStageFailures(isFailure: boolean, stages: Stage[]) {
    const stageIndex = getCurrentStageIndex();
    const isSteadyStage = (stageIndex % 2 == 1);
    if (isSteadyStage) {
        stageFailureRate.add(
            isFailure,
            { target_rpm: stages[stageIndex].target.toString(), stage_profile: 'steady' }
        );
    }
}

export function createStages(params: StageConfig): Stage[] {
    const stages = [];
    var target = params.startRatePerMinute;
    for (var i = 0; i < params.heldStageCount; ++i) {
        stages.push({ target: target, duration: params.rampStageDuration });
        stages.push({ target: target, duration: params.heldStageDuration });
        target += params.rateIncrement;
    }
    return stages;
}

export function createStageThresholds(params: StageConfig, requestSloMillis: number) {
    const thresholds: any = {
        http_req_duration: [{
            threshold: `p(95) < ${requestSloMillis}`,
            abortOnFail: true,
            delayAbortEval: `${Math.ceil(requestSloMillis / 2)}`
        }],
        // Overall there should be less than 5% failures.
        // We use throughput_stage_failed instead of http_req_failed for this threshold,
        // as the former is more generic and considers long durations as failed.
        throughput_stage_failed: [{ threshold: 'rate <= 0.05', abortOnFail: true }],
    };

    var target = params.startRatePerMinute;
    for (var i = 0; i < params.heldStageCount; ++i) {
        const key = `throughput_stage_failed{stage_profile: steady, target_rpm: ${target}}`;
        // The threshold for i == 0 is real, the first stage should not have failures.
        // The threshold for the other stages are pseudo-thresholds, they will always pass as the
        // rate is always at least zero. They show the failure rate per stage in the output.
        thresholds[key] = [
            i == 0 ? { threshold: 'rate <= 0', abortOnFail: true } : 'rate >= 0'
        ];
        target += params.rateIncrement;
    }

    return thresholds
}

export function getTargetRpmTags(stages: Stage[]): object {
    const stageIndex = getCurrentStageIndex();
    const isSteadyStage = (stageIndex % 2 == 1);
    return {
        target_rpm: stages[stageIndex].target.toString(),
        stage_profile: isSteadyStage ? 'steady' : 'ramp-up',
    };
}

export function logObject(...objects: object[]) {
    for (const obj of objects) {
        for (const [key, value] of Object.entries(obj)) {
            console.log(`${key}: ${value}`);
        }
    }
}

const testActive = new Gauge("test_active", /*isTime=*/false);
const startTime = new Gauge("test_start_timestamp_seconds", /*isTime=*/true);
const endTime = new Gauge("test_end_timestamp_seconds", /*isTime=*/true);

/**
 * Record the test start time, should only be called once in setup().
 */
export function recordStartTime() {
    startTime.add(Date.now() / 1000);
    testActive.add(1);
}

/**
 * Record the test end time, should only be called once in teardown().
 */
export function recordEndTime() {
    endTime.add(Date.now() / 1000);
    testActive.add(0);
}
