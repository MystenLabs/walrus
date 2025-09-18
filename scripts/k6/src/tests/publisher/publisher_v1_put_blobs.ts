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
import { PUBLISHER_URL, PAYLOAD_SOURCE_FILE, REDIS_URL } from '../../config/environment.ts'
import { PutBlobOptions, putBlob } from '../../flows/publisher.ts'
import { open } from 'k6/experimental/fs';
import { parseHumanFileSize } from "../../lib/utils.ts"
import { BlobHistory } from "../../lib/blob_history.ts"
import { check } from 'k6';


/**
 * Number of blobs to store
 * @default 20
 */
const BLOBS_TO_STORE: number = parseInt(__ENV.BLOBS_TO_STORE || '20');

/**
 * The number of virtual users that will repeatedly store blobs.
 * @default 1
 */
const VUS: number = parseInt(__ENV.VUS || '1');

/**
 * The payload size of each request with a Ki or Mi or Gi suffix.
 * @default '1Ki'
 */
const PAYLOAD_SIZE: string = __ENV.PAYLOAD_SIZE || '1Ki';

/**
 * The timeout for storing each blob.
 * @default 5m
 */
const TIMEOUT: string = __ENV.TIMEOUT || '5m';


export const options = {
    scenarios: {
        "putBlobs": {
            executor: 'shared-iterations',
            vus: VUS,
            iterations: BLOBS_TO_STORE,
            maxDuration: "15m",
        }
    },

    tags: {
        "blobs": `${BLOBS_TO_STORE}`,
        "payload-size": `${PAYLOAD_SIZE}`,
    },

    // Skip TLS verification for self-signed certs.
    insecureSkipTLSVerify: true,
};

const dataFile = await open(PAYLOAD_SOURCE_FILE);
const blobHistory = new BlobHistory(REDIS_URL);

export function setup() {
    console.log('');
    console.log(`Publisher URL: ${PUBLISHER_URL}`);
    console.log(`Blobs to store: ${BLOBS_TO_STORE}`);
    console.log(`Virtual users: ${VUS}`);
    console.log(`Data file path: ${PAYLOAD_SOURCE_FILE}`);
    console.log(`Payload size: ${PAYLOAD_SIZE} (${parseHumanFileSize(PAYLOAD_SIZE)} B)`);
    console.log(`Blob store timeout: ${TIMEOUT}`);
    if (REDIS_URL != undefined) {
        console.log(`Blob history written to: ${REDIS_URL}`);
    }
}

export default async function () {
    const payloadSize = parseHumanFileSize(PAYLOAD_SIZE);
    const response = await putBlob(
        dataFile, PUBLISHER_URL, new PutBlobOptions(payloadSize, payloadSize, TIMEOUT)
    );

    check(response, {
        'is status 200': (r) => r.status === 200,
    });

    await blobHistory.maybeRecordFromResponse(PAYLOAD_SIZE, response);
}

export async function teardown() {
    const blobIdCount = await blobHistory.len(PAYLOAD_SIZE);
    if (blobIdCount != null) {
        console.log(`Total Blob IDs stored under key "${PAYLOAD_SIZE}": ${blobIdCount}`);
    }
}
