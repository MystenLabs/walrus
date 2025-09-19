// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

// @ts-ignore
import { randomIntBetween } from 'https://jslib.k6.io/k6-utils/1.2.0/index.js'
// @ts-ignore
import { expect } from 'https://jslib.k6.io/k6-testing/0.5.0/index.js';
import { File, SeekMode } from 'k6/experimental/fs';

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


/**
 * Calculate the `startTime` offset for a scenario.
 * @param index - Zero based index of the scenario's run order.
 * @param scenarioDurationSeconds - Number of seconds each scenario runs for.
 * @param [gapSeconds=5] - Time between one scenario ending and another starting.
 * @returns The startTime offset in seconds.
 */
export function scenarioStartTimeSeconds(
    index: number,
    scenarioDurationSeconds: number,
    gapSeconds: number = 5
): number {
    if (index == 0) {
        return 0;
    } else {
        return (index * (scenarioDurationSeconds + gapSeconds))
    }
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

/**
 * Loads the plan if specified.
 *
 * If the specified plan does not exist an error is raised.
 */
function loadPlan(path: string, plan?: string): Record<string, any> {
    if (plan == undefined) {
        return {};
    }
    const plans = JSON.parse(open(path));
    if (!(plan in plans)) {
        throw new Error(`The requested plan "${plan}" is not present in "${path}"`);
    } else {
        return plans[plan];
    }
}


type Parameters = Record<string, string | number>;

/**
 * For each camelCase property in `example`, load the corresponding UPPER_SNAKE_CASE
 * value from __ENV and return it.
 */
function loadEnv(keysAndTypes: Record<string, string>): Parameters {
    const output: Parameters = {}

    for (const [key, keyType] of Object.entries(keysAndTypes)) {
        const value = __ENV[key.replace(/([a-z0-9])([A-Z])/g, "$1_$2").toUpperCase()];
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
 * Loads parameters from a plan and __ENV.
 *
 * Variables set in __ENV take precedence over those in the plan.
 *
 * @param defaults - The default parameters. The keys in the `defaults` are converted from camelCase
 * to UPPER_SNAKE_CASE and used as keys to fetch any set values in __ENV.
 * @param planFilePath - A path within the config/ directory that contains the plans. If specified,
 * then the environment variable __ENV.PLAN is checked for a plan name and the defaults are updated
 * with the named plan.
 * @returns The loaded parameters.
 */
export function loadParameters<T extends object>(defaults: T, planFilePath?: string): T {
    var output = defaults;

    if (planFilePath != undefined) {
        output = Object.assign(
            output,
            loadPlan(import.meta.resolve("../config/" + planFilePath), __ENV.PLAN),
        );
    }

    const keysAndTypes = Object.fromEntries(
        Object.entries(defaults).map(([key, value]) => [key, typeof (value)])
    );
    return Object.assign(output, loadEnv(keysAndTypes))
}
