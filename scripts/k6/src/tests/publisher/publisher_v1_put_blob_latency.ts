// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0
//
// Measures the latency of putting file via a publisher on Walrus, by storing a
// fixed number of files of a given size.
//
import { loadEnvironment } from '../../config/environment.ts'
import { putBlob } from '../../flows/publisher.ts'
import * as fs from 'k6/experimental/fs';
import { parseHumanFileSize, loadParameters } from "../../lib/utils.ts"
import { BlobHistory } from "../../lib/blob_history.ts"
// @ts-ignore
import { expect } from 'https://jslib.k6.io/k6-testing/0.5.0/index.js';


/**
 * Test parameters read from the environment in UPPER_CASE with a WALRUS_K6_ prefix.
 */
interface TestParameters {
    /** Number of blobs to store. */
    blobsToStore: number,
    /** The number of virtual users that will repeatedly store blobs. */
    maxConcurrency: number,
    /** The payload size of each request with a Ki or Mi or Gi suffix. */
    payloadSize: string,
    /** The timeout for storing each blob. */
    timeout: string,
}

const env = loadEnvironment();
const params = loadParameters<TestParameters>({
    blobsToStore: 10,
    maxConcurrency: 3,
    payloadSize: "1Ki",
    timeout: "10m",
});
const dataFile = await fs.open(env.payloadSourceFile);
const blobHistory = new BlobHistory(env.redisUrl);

export const options = {
    scenarios: {
        "putBlobs": {
            executor: 'shared-iterations',
            vus: params.maxConcurrency,
            iterations: params.blobsToStore,
            maxDuration: "15m",
        }
    },
    insecureSkipTLSVerify: true,
};


export function setup(): number {
    const fileSizeBytes = parseHumanFileSize(params.payloadSize);

    console.log('');
    console.log(`Publisher URL: ${env.publisherUrl}`);
    console.log(`Blobs to store: ${params.blobsToStore}`);
    console.log(`Concurrent stores: ${params.maxConcurrency}`);
    console.log(`Data file path: ${env.payloadSourceFile}`);
    console.log(`Payload size: ${params.payloadSize} (${fileSizeBytes} B)`);
    console.log(`Blob store timeout: ${params.timeout}`);
    if (env.redisUrl) {
        console.log(`Blob history written to: ${env.redisUrl}`);
    }

    return fileSizeBytes;
}

export default async function (fileSizeBytes: number) {
    const response = await putBlob(
        dataFile, env.publisherUrl, fileSizeBytes, { timeout: params.timeout }
    );

    expect(response.status).toBe(200);
    expect(response.json()).toHaveProperty('newlyCreated');

    await blobHistory.maybeRecordFromResponse(params.payloadSize, response);
}
