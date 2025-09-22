// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0
//
// Store a fixed number of files of a specified size on Walrus.
//
// For example,
// ```
// k6 run --env VUS=3 --env BLOBS_TO_STORE=20 --env PAYLOAD_SIZE=10Mi \
//   --env ENVIRONMENT=localhost publisher_v1_put_blobs.ts
// ```
// stores 20 files, each 10 MiB, using 3 concurrent clients in parallel, via
// a publisher already running on localhost.
//
// See `environment.ts` for ENVIRONMENT defaults.
import { DEFAULT_ENVIRONMENT, loadEnvironment } from '../../config/environment.ts'
import { PutBlobOptions, putBlob } from '../../flows/publisher.ts'
import * as fs from 'k6/experimental/fs';
import { parseHumanFileSize, loadParameters, getTestIdTags } from "../../lib/utils.ts"
import { BlobHistory } from "../../lib/blob_history.ts"
import { check } from 'k6';


interface TestParameters {
    /** Number of blobs to store. */
    blobsToStore: number,
    /** The number of virtual users that will repeatedly store blobs. */
    vus: number,
    /** The payload size of each request with a Ki or Mi or Gi suffix. */
    payloadSize: string,
    /** The timeout for storing each blob. */
    timeout: string,
    /** The environment in which the test is running. */
    environment: string,
}

const params = loadParameters<TestParameters>(
    {
        blobsToStore: 20,
        vus: 1,
        payloadSize: "1Ki",
        timeout: "1m",
        environment: DEFAULT_ENVIRONMENT,
    },
    "publisher/v1_put_blobs.plans.json"
);

const env = loadEnvironment(params.environment);

const dataFile = await fs.open(env.payloadSourceFile);

const blobHistory = new BlobHistory(env.redisUrl);

export const options = {
    scenarios: {
        "putBlobs": {
            executor: 'shared-iterations',
            vus: params.vus,
            iterations: params.blobsToStore,
            maxDuration: "15m",
        }
    },

    tags: {
        ...getTestIdTags(),
        payload_size: `${params.payloadSize}`,
    },

    // Skip TLS verification for self-signed certs.
    insecureSkipTLSVerify: true,
};


export function setup() {
    const humanFileSize = parseHumanFileSize(params.payloadSize);

    console.log('');
    console.log(`Publisher URL: ${env.publisherUrl}`);
    console.log(`Blobs to store: ${params.blobsToStore}`);
    console.log(`Virtual users: ${params.vus}`);
    console.log(`Data file path: ${env.payloadSourceFile}`);
    console.log(`Payload size: ${params.payloadSize} (${humanFileSize} B)`);
    console.log(`Blob store timeout: ${params.timeout}`);
    if (env.redisUrl != undefined) {
        console.log(`Blob history written to: ${env.redisUrl}`);
    }
}

export default async function () {
    const payloadSize = parseHumanFileSize(params.payloadSize);
    const response = await putBlob(
        dataFile, env.publisherUrl, new PutBlobOptions(payloadSize, payloadSize, params.timeout)
    );

    check(response, {
        'is status 200': (r) => r.status === 200,
    });

    await blobHistory.maybeRecordFromResponse(params.payloadSize, response);
}

export async function teardown() {
    const blobIdCount = await blobHistory.len(params.payloadSize);
    if (blobIdCount != null) {
        console.log(`Total Blob IDs stored under key "${params.payloadSize}": ${blobIdCount}`);
    }
}
