// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

// @ts-check
import { randomRange, readFileRange } from "../lib/utils.js";
import { File } from 'k6/experimental/fs';
import http from 'k6/http';

/** Options provided to the `putBlob` method. */
export class PutBlobOptions {
    /**
     * Create a new set of options.
     * @param {number} minLength  - The minimum size of the blob to be stored.
     * @param {number} [maxLength=minLength]  - The maximum size of the blob to be stored.
     * @param {string} [timeout='120s'] - Timeout for the request.
     */
    constructor(minLength, maxLength = minLength, timeout = '120s') {
        this.minLength = minLength;
        this.maxLength = maxLength;
        this.timeout = timeout;
    }
}

/**
 * Stores a blob read from `dataFile` via a publisher.
 * @param {File} dataFile - The file from which to read a chunk of data to send.
 * @param {string} publisherUrl - Base URL of the Walrus publisher to which to send.
 * @param {PutBlobOptions} options - Options to configure the put operation.
 */
export async function putBlob(
    dataFile,
    publisherUrl,
    options,
) {
    const dataFileLength = (await dataFile.stat()).size;
    const [dataStart, dataLength] = randomRange(
        options.minLength, options.maxLength, dataFileLength
    );
    const blob = await readFileRange(dataFile, dataStart, dataLength);

    http.put(`${publisherUrl}/v1/blobs`, blob, { timeout: options.timeout });
}
