// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

import { PUBLISHER_URL, PAYLOAD_SOURCE_FILE } from '../../config/environment.ts'
import { PutBlobOptions, putBlob } from '../../flows/publisher.ts'
import { open } from 'k6/experimental/fs';
import { parseHumanFileSize } from "../../lib/utils.ts"

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

export function setup() {
    console.log('');
    console.log(`Publisher URL: ${PUBLISHER_URL}`);
    console.log(`Blobs to store: ${BLOBS_TO_STORE}`);
    console.log(`Virtual users: ${VUS}`);
    console.log(`Data file path: ${PAYLOAD_SOURCE_FILE}`);
    console.log(`Payload size: ${PAYLOAD_SIZE} (${parseHumanFileSize(PAYLOAD_SIZE)} B)`);
    console.log(`Blob store timeout: ${TIMEOUT}`);
}

export default async function () {
    const payloadSize = parseHumanFileSize(PAYLOAD_SIZE);
    await putBlob(dataFile, PUBLISHER_URL, new PutBlobOptions(payloadSize, payloadSize, TIMEOUT))
}
