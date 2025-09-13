// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

/**
 * A configuration for a test environment.
 */
interface EnvironmentConfig {
    /**
     * The default URL of the publisher for the environment.
     */
    publisherUrl: string
    /**
     * The default path of the data file for the environment.
     */
    payloadSourceFile: string,
}

/**
 * Default configurations for various running environments.
 */
const ENVIRONMENT_DEFAULTS: { [index: string]: EnvironmentConfig } = {
    "localhost": {
        "publisherUrl": "http://localhost:31415",
        "payloadSourceFile": "../../../data.bin" // Within the k6 folder
    },
    "walrus-performance-network": {
        "publisherUrl": "http://walrus-publisher-0.walrus-publisher:31415",
        "payloadSourceFile": "/opt/k6/data/data.bin"
    },
    "walrus-testnet": {
        "publisherUrl": "https://publisher.walrus-testnet.walrus.space",
        "payloadSourceFile": "../../../data.bin" // Within the k6 folder
    }
}

const ENVIRONMENT = __ENV.ENVIRONMENT || "walrus-testnet";

/**
 * The URL of the publisher to use.
 */
export const PUBLISHER_URL: string = __ENV.PUBLISHER_URL
    || ENVIRONMENT_DEFAULTS[ENVIRONMENT].publisherUrl;

/**
 * The path to a file that is used as the source of all the blobs.
 * Each blob is taken as a slice from the raw bytes of the file.
 */
export const PAYLOAD_SOURCE_FILE: string = __ENV.PAYLOAD_SOURCE_FILE
    || ENVIRONMENT_DEFAULTS[ENVIRONMENT].payloadSourceFile;
