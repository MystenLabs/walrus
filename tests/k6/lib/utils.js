// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

// @ts-check
import { randomIntBetween } from 'https://jslib.k6.io/k6-utils/1.2.0/index.js';
import { expect } from 'https://jslib.k6.io/k6-testing/0.5.0/index.js';
import { File, SeekMode } from 'k6/experimental/fs';

/**
 * Returns a random range `[start, length]` on the interval `[0, limit)`.
 * @param {number} limit - The maximum value for `start + length`, denoting the highest possible
 * bound of the range.
 * @param {number} minLength  - The minimum length of the range.
 * @param {number} maxLength  - The maximum length of the range.
 * @returns {[number, number]} - The tuple of start index and length of the range.
 */
export function randomRange(minLength, maxLength, limit) {
    expect(maxLength).toBeLessThanOrEqual(limit);

    const start = randomIntBetween(0, limit - minLength);
    const length = randomIntBetween(minLength, Math.min(limit - start, maxLength));
    return [start, length];
}

/**
 * Asynchronously reads and returns a byte range from the file.
 *
 * @throws Throws an error if if the indicated range is beyond the file length.
 *
 * @param {File} dataFile - The data file from which to read.
 * @param {number} start - The start of the range to read.
 * @param {number} length - The length of the range to read.
 * @returns {Promise<Uint8Array>} - The read data.
 */
export async function readFileRange(dataFile, start, length) {
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
 * @param {number} index - Zero based index of the scenario's run order.
 * @param {number} scenarioDurationSeconds - Number of seconds each scenario runs for.
 * @param {number} [gapSeconds=5] - Time between one scenario ending and another starting.
 * @returns {number} The startTime offset in seconds.
 */
export function scenarioStartTimeSeconds(index, scenarioDurationSeconds, gapSeconds = 5) {
    if (index == 0) {
        return 0;
    } else {
        return (index * (scenarioDurationSeconds + gapSeconds))
    }
}

/**
 * Format bytes as human-readable text.
 *
 * @param {number} bytes Number of bytes.
 * @param {number} [fractionDigits=1] Number of decimal points to which to round.
 * @returns {string} Formatted string.
 */
export function humanFileSize(bytes, fractionDigits = 1) {
    const threshold = 1024;

    if (Math.abs(bytes) < threshold) {
        return bytes + ' B';
    }

    const units = ['KiB', 'MiB', 'GiB'];
    let u = -1;
    const r = 10 ** fractionDigits;

    do {
        bytes /= threshold;
        ++u;
    } while (Math.round(Math.abs(bytes) * r) / r >= threshold && u < units.length - 1);

    return bytes.toFixed(fractionDigits) + ' ' + units[u];
}
