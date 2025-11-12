// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0
import { loadRandomData, ensure } from "../lib/utils.ts";
import { MAX_FILES_IN_QUILT, BYTES_PER_KIBIBYTE } from "../lib/constants.ts";
import { File } from 'k6/experimental/fs';
import http, { RequestBody } from 'k6/http';
import { randomIntBetween }
    // @ts-ignore
    from 'https://jslib.k6.io/k6-utils/1.6.0/index.js'
import { Trend } from "k6/metrics";

/**
 * Information about a stored quilt patch returned by the publisher.
 */
export interface StoredQuiltPatch {
    identifier: string,
    quiltPatchId: string,
    range?: number[]
}

const quiltFileSizeTrend = new Trend('quilt_file_size_bytes', /*isTime=*/false);

/**
 * Stores a blob read from `dataFile` via a publisher.
 * @param dataFile - The file from which to read a chunk of data to send.
 * @param publisherUrl - Base URL of the Walrus publisher to which to send.
 * @param fileSizeBytes - The size of the file in bytes to put.
 * @param params - Parameters passed directly to the `http.put` method.
 */
export async function putBlob(
    dataFile: File,
    publisherUrl: string,
    fileSizeBytes: number,
    params?: Record<string, any>,
) {
    const blob = await loadRandomData(dataFile, fileSizeBytes);
    return http.put(`${publisherUrl}/v1/blobs`, blob, params);
}

/**
 * Options provided to the `postQuilt` method.
 */
export interface PostQuiltOptions {
    /**
     * The desired total size of all the files in the quilt, in bytes.
     *
     * The total, final size may deviate from the desired size, as the sizes of each
     * file will be multiples of 1 KiB.
     */
    totalFileSizeBytes: number
    /**
     * The number of files in the quilt, at most 666. If set to a negative
     * number, the number of files will be randomly selected.
     */
    quiltFileCount: number
    /**
     * Whether file sizes are assigned uniformly or at random. Selected sizes
     * will be multiples of 1 KiB and at most maxQuiltFileSizeKib.
     */
    quiltFileSizeAssignment: "uniform" | "random",
    /** The maximum allowed size in KiB for a file in the Quilt. */
    maxQuiltFileSizeKib: number
}

/**
 * Stores a quilt via the publisher.
 *
 * The number of files and their sizes are computed based on the provided options, and the data
 * for each file is taken from the provided data file.
 * @param dataFile - The file from which to read a chunk of data to send.
 * @param publisherUrl - Base URL of the Walrus publisher to which to send.
 * @param options - Options to configure the construction of the quilt.
 * @param params - http params passed directly to the `http.put` method.
 */
export async function putQuilt(
    dataFile: File,
    publisherUrl: string,
    options: PostQuiltOptions,
    params?: Record<string, any>,
) {
    const body = await quiltPostBody(dataFile, options);
    return http.put(`${publisherUrl}/v1/quilts`, body, params);
}

interface FileSizes {
    sizesBytes: number[]
    finalTotalFileSizeBytes: number,
}

async function quiltPostBody(dataFile: File, options: PostQuiltOptions): Promise<RequestBody> {
    ensure(
        options.quiltFileCount <= MAX_FILES_IN_QUILT,
        `${options.quiltFileCount} is larger than the maximum number of files in a quilt (666)`
    );
    const fileSizes = (options.quiltFileSizeAssignment == "random") ?
        randomQuiltFileSizes(options) :
        uniformQuiltFileSizes(options);

    const httpBody: { [index: string]: any } = {};

    for (const [index, fileSizeBytes] of fileSizes.sizesBytes.entries()) {
        quiltFileSizeTrend.add(fileSizeBytes);

        const data = await loadRandomData(dataFile, fileSizeBytes);
        const buffer = data.slice().buffer;
        httpBody[`file-${index}`] = http.file(buffer, `file-${index}.bin`);
    }
    return httpBody
}

function getOrSampleFileCount(options: PostQuiltOptions) {
    if (options.quiltFileCount >= 1) {
        return options.quiltFileCount;
    }

    const totalFileSizeKib = Math.round(options.totalFileSizeBytes / BYTES_PER_KIBIBYTE);
    const maxFiles = Math.min(MAX_FILES_IN_QUILT, totalFileSizeKib);
    const minFiles = Math.ceil(totalFileSizeKib / options.maxQuiltFileSizeKib);
    ensure(
        maxFiles >= minFiles,
        `cannot construct a quilt of files totalling ${totalFileSizeKib} KiB, ` +
        `with each file at most ${options.maxQuiltFileSizeKib} KiB`
    );

    return randomIntBetween(minFiles, maxFiles);
}

function randomQuiltFileSizes(options: PostQuiltOptions): FileSizes {
    const fileSizes = uniformQuiltFileSizes(options);
    const fileCount = fileSizes.sizesBytes.length;
    const maxQuiltFileSizeBytes = options.maxQuiltFileSizeKib * BYTES_PER_KIBIBYTE;

    for (var i = 0; i < 1000; i++) {
        const reduceIndex = randomIntBetween(0, fileCount - 1);
        const increaseIndex = randomIntBetween(0, fileCount - 1);
        if (reduceIndex == increaseIndex) {
            continue;
        }

        const maxReduce = fileSizes.sizesBytes[reduceIndex] - 1;
        const maxIncrease = maxQuiltFileSizeBytes - fileSizes.sizesBytes[increaseIndex];
        const change = randomIntBetween(0, Math.min(maxReduce, maxIncrease));

        fileSizes.sizesBytes[reduceIndex] -= change;
        fileSizes.sizesBytes[increaseIndex] += change;
    }

    return fileSizes;
}

function uniformQuiltFileSizes(options: PostQuiltOptions): FileSizes {
    const { totalFileSizeBytes, maxQuiltFileSizeKib } = options;
    const totalFileSizeKib = Math.round(totalFileSizeBytes / BYTES_PER_KIBIBYTE);
    const quiltFileCount = getOrSampleFileCount(options);

    ensure(
        totalFileSizeKib >= quiltFileCount,
        "total file size must be at least 1KiB * number of files"
    );

    const sizeOfEachFileKib = Math.round(totalFileSizeKib / quiltFileCount);
    ensure(
        sizeOfEachFileKib <= maxQuiltFileSizeKib,
        `the requested options ${options} result in files of size ${sizeOfEachFileKib} KiB ` +
        `which is larger than the maximum allowed size of ${maxQuiltFileSizeKib} KiB`
    )

    const sizeOfEachFileBytes = sizeOfEachFileKib * BYTES_PER_KIBIBYTE;
    return {
        sizesBytes: Array(quiltFileCount).fill(sizeOfEachFileBytes),
        finalTotalFileSizeBytes: (sizeOfEachFileBytes * quiltFileCount),
    }
}
