// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

/**
 * Default configurations for various running environments.
 */
const ENVIRONMENT_DEFAULTS = {
    "localhost": {
        "publisherUrl": "http://localhost:31415",
        "payloadSourceFile": "data.bin"
    },
    "walrus-performance-network": {
        "publisherUrl": "http://walrus-publisher-0.walrus-publisher:31415",
        "payloadSourceFile": "/opt/k6/data/data.bin"
    }
}

const ENVIRONMENT = __ENV.ENVIRONMENT || "localhost";

/**
 * The URL of the publisher to use.
 * @type {string}
 */
export const PUBLISHER_URL = __ENV.PUBLISHER_URL
    || ENVIRONMENT_DEFAULTS[ENVIRONMENT].publisherUrl;

/**
 * The path to a file that is used as the source of all the blobs.
 * Each blob is taken as a slice from the raw bytes of the file.
 * @type {string}
 */
export const PAYLOAD_SOURCE_FILE = __ENV.PAYLOAD_SOURCE_FILE
    || ENVIRONMENT_DEFAULTS[ENVIRONMENT].payloadSourceFile;
