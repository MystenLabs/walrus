// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

// @ts-check
import { randomRange, readFileRange } from "../lib/utils.ts";
import { File } from 'k6/experimental/fs';
import http from 'k6/http';

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
    const blob = await readFileRange(dataFile, dataStart, dataLength);

    return http.put(`${publisherUrl}/v1/blobs`, blob, { timeout: options.timeout });
}
