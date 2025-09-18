// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0
//
// Retrieve a fixed number of blobs from Walrus and report the metrics.
//
// The blob IDs to fetch are read from the Redis database located at `REDIS_URL`
// under the key with the same name as `PAYLOAD_SIZE`.
//
// For example,
// ```
// k6 run --env VUS=3 --env BLOBS_TO_READ=20 --env PAYLOAD_SIZE=10Mi \
//   --env ENVIRONMENT=localhost aggregator_v1_get_blob_latency.ts
// ```
//
// See `environment.ts` for ENVIRONMENT defaults.
import { REDIS_URL, AGGREGATOR_URL } from '../../config/environment.ts'
import { parseHumanFileSize } from "../../lib/utils.ts"
import { BlobHistory } from "../../lib/blob_history.ts"
import { check } from 'k6';
import exec from 'k6/execution';
// @ts-ignore
import { randomIntBetween } from 'https://jslib.k6.io/k6-utils/1.2.0/index.js'
import { getBlob } from '../../flows/aggregator.ts';

/**
 * Number of blobs to read.
 * @default 20
 */
const BLOBS_TO_READ: number = parseInt(__ENV.BLOBS_TO_READ || '20');

/**
 * The number of virtual users that will repeatedly store blobs.
 * @default 1
 */
const VUS: number = parseInt(__ENV.VUS || '1');

/**
 * Used as the key into the redis key-value store to fetch blob IDs
 * corresponding to blobs of the given size.
 * @default '1Ki'
 */
const PAYLOAD_SIZE: string = __ENV.PAYLOAD_SIZE || '1Ki';

/**
 * The timeout for fetching each blob.
 * @default 5m
 */
const TIMEOUT: string = __ENV.TIMEOUT || '5m';


export const options = {
    scenarios: {
        "getBlobs": {
            executor: 'shared-iterations',
            vus: VUS,
            iterations: BLOBS_TO_READ,
            maxDuration: "15m",
        }
    },

    // Skip TLS verification for self-signed certs.
    insecureSkipTLSVerify: true,
};

const blobHistory = new BlobHistory(REDIS_URL);

export async function setup(): Promise<string[]> {
    console.log('');
    console.log(`Aggregator URL: ${AGGREGATOR_URL}`);
    console.log(`Blobs to read: ${BLOBS_TO_READ}`);
    console.log(`Virtual users: ${VUS}`);
    console.log(`Payload size: ${PAYLOAD_SIZE} (${parseHumanFileSize(PAYLOAD_SIZE)} B)`);
    console.log(`Blob read timeout: ${TIMEOUT}`);

    if (REDIS_URL == undefined) {
        exec.test.abort("REDIS_URL must be defined");
    }

    const blobIds = await blobHistory.list(PAYLOAD_SIZE);
    const count = blobIds.length;
    console.log(`Blob IDs: source="${REDIS_URL}", key="${PAYLOAD_SIZE}", count=${count}`);

    return blobIds
}

export default async function (blobIds: string[]) {
    const blobId = blobIds[randomIntBetween(0, blobIds.length - 1)];
    const response = await getBlob(AGGREGATOR_URL, blobId, TIMEOUT);

    check(response, {
        'is status 200': (r) => r.status === 200,
    });
}
