// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

// @ts-check
import {
    htmlReport
} from "https://raw.githubusercontent.com/benc-uk/k6-reporter/main/dist/bundle.js";
import { textSummary } from "https://jslib.k6.io/k6-summary/0.0.1/index.js";
import { PutBlobOptions, putBlob } from "../../flows/publisher.js"
import { PUBLISHER_URL, PAYLOAD_SOURCE_FILE } from "../../config/environment.js"
import { open } from 'k6/experimental/fs';

/**
 * Number of blobs to store
 * @type {number}
 * @default 20
 */
const BLOBS_TO_STORE = parseInt(__ENV.BLOBS_TO_STORE || '20');

/**
 * The number of virtual users that will repeatedly store blobs.
 * @type {number}
 * @default 1
 */
const VUS = parseInt(__ENV.VUS || '1');

/**
 * The payload size of each request with a Ki or Mi or Gi suffix.
 *
 * @type {string}
 * @example 100Ki
 */
const PAYLOAD_SIZE = __ENV.PAYLOAD_SIZE || '1Ki';

const PAYLOAD_SIZE_CONVERSION = {
    '1Ki': 1024,
    '100Ki': 100 * 1024,
    '1Mi': 1024 * 1024,
    '10Mi': 10 * 1024 * 1024,
    '100Mi': 100 * 1024 * 1024,
    '500Mi': 500 * 1024 * 1024,
    '1Gi': 1024 * 1024 * 1024,
}


export const options = {
    scenarios: {
        "putBlobs": {
            executor: 'shared-iterations',
            vus: VUS,
            iterations: BLOBS_TO_STORE,
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
    console.log(`Payload size: ${PAYLOAD_SIZE}`);


}

export default async function () {
    const payloadSize = PAYLOAD_SIZE_CONVERSION[PAYLOAD_SIZE];
    await putBlob(dataFile, PUBLISHER_URL, new PutBlobOptions(payloadSize))
}

export function handleSummary(data) {
    return {
        "result.html": htmlReport(data),
        stdout: textSummary(data, { indent: " ", enableColors: true }),
    };
}
