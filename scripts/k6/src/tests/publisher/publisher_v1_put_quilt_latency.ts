// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0
//
// Measures the latency of putting multiple quilt files via a publisher on Walrus.
//
// Given the total file size of the overall quilt and the number of files in the quilt (optional)
// the script creates files totalling approximately that size and stores them in the quilt.
//
// The sizes of the files may be selected to be uniform or randomly selected.
//
import { loadEnvironment } from '../../config/environment.ts'
import { putQuilt } from '../../flows/publisher.ts'
import * as fs from 'k6/experimental/fs';
import { parseHumanFileSize, loadParameters, logObject, recordEndTime, recordStartTime }
    from "../../lib/utils.ts"
import { BlobHistory } from "../../lib/blob_history.ts"
import { expect }
    // @ts-ignore
    from 'https://jslib.k6.io/k6-testing/0.5.0/index.js';
import { BYTES_PER_KIBIBYTE } from "../../lib/constants.ts";


/**
 * Test parameters read from the environment in UPPER_CASE with a WALRUS_K6_ prefix.
 */
interface TestParameters {
    /** Number of blobs to store. */
    quiltsToStore: number,
    /** The payload size of each request with a Ki or Mi or Gi suffix. */
    totalFileSize: string,
    /** The number of files in the quilt. Set to -1 for a random number of files. **/
    quiltFileCount: number,
    /** How file sizes are assigned. **/
    quiltFileSizeAssignment: "uniform" | "random",
    /** The maximum size of a file in the quilt with a Ki or Mi or Gi suffix. */
    maxQuiltFileSize: string,
    /** The number of virtual users that will repeatedly store blobs. */
    maxConcurrency: number,
    /** The timeout for storing each blob. */
    timeout: string,
}

/** Common environment setting like URLs, loaded from __ENV with defaults. */
const env = loadEnvironment();
/** File providing random bytes to be used for blob files. */
const dataFile = await fs.open(env.payloadSourceFile);
/** Handle for storing the IDs of blobs written to the network. */
const blobHistory = new BlobHistory(env.redisUrl);
/** Parameters from __ENV with defaults. */
const params = loadParameters<TestParameters>({
    quiltsToStore: 1,
    totalFileSize: "10Mi",
    quiltFileCount: -1,
    quiltFileSizeAssignment: "uniform",
    maxQuiltFileSize: "1Mi",
    maxConcurrency: 1,
    timeout: "15m",
});

export const options = {
    scenarios: {
        "putBlobs": {
            executor: 'shared-iterations',
            vus: params.maxConcurrency,
            iterations: params.quiltsToStore,
            maxDuration: "60m",
        }
    },
    insecureSkipTLSVerify: true,
};


export function setup() {
    recordStartTime();
    logObject(params, env);
    return {
        totalFileSizeBytes: parseHumanFileSize(params.totalFileSize),
        maxQuiltFileSizeKib: Math.round(
            parseHumanFileSize(params.maxQuiltFileSize) / BYTES_PER_KIBIBYTE
        )
    };
}

export default async function ({ totalFileSizeBytes, maxQuiltFileSizeKib }: any) {
    const options = {
        totalFileSizeBytes: totalFileSizeBytes,
        quiltFileCount: params.quiltFileCount,
        quiltFileSizeAssignment: params.quiltFileSizeAssignment,
        maxQuiltFileSizeKib: maxQuiltFileSizeKib
    }

    const response = await putQuilt(
        dataFile, env.publisherUrl, options, { timeout: params.timeout }
    );
    expect(response.status).toBe(200);

    const keyPrefix = `quilt:${params.totalFileSize}:${params.quiltFileSizeAssignment}`;
    await blobHistory.recordQuiltPatchesFromResponse(`${keyPrefix}:patches`, response);
    await blobHistory.recordQuiltFileIdsFromResponse(`${keyPrefix}:file_ids`, response);
}

export function teardown() {
    recordEndTime();
}
