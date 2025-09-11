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
