// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

import { perturbData, randomRange, readFileRange } from "../lib/utils.ts";
import { File } from 'k6/experimental/fs';
import http from 'k6/http';
// @ts-ignore
import { randomIntBetween } from 'https://jslib.k6.io/k6-utils/1.2.0/index.js'

/**
 * Options provided to the `putBlob` method.
 */
export class PutBlobOptions {
    /** The minimum size of the blob to be stored. */
    minLength: number
    /**
     * The maximum size of the blob to be stored.
     * @default minLength
     */
    maxLength: number
    /**
     * Timeout for the request
     * @default 120s
     */
    timeout: string

    constructor(minLength: number, maxLength: number = minLength, timeout: string = '120s') {
        this.minLength = minLength;
        this.maxLength = maxLength;
        this.timeout = timeout;
    }
}

/**
 * Stores a blob read from `dataFile` via a publisher.
 * @param dataFile - The file from which to read a chunk of data to send.
 * @param publisherUrl - Base URL of the Walrus publisher to which to send.
 * @param options - Options to configure the put operation.
 */
export async function putBlob(
    dataFile: File,
    publisherUrl: string,
    options: PutBlobOptions,
) {
    const dataFileLength = (await dataFile.stat()).size;
    const [dataStart, dataLength] = randomRange(
        options.minLength, options.maxLength, dataFileLength
    );

    // A 1GiB file can only provide ~1000 unique 100MiB sub-ranges. To increase
    // the number of 'files' that we can get from the source data, we therefore
    // mutate some bytes.
    const blob = perturbData(await readFileRange(dataFile, dataStart, dataLength));

    return http.put(`${publisherUrl}/v1/blobs`, blob, { timeout: options.timeout });
}
