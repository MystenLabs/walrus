// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

type RedisUrl = `redis://${string}`

/**
 * A configuration for a test environment.
 */
interface EnvironmentConfig {
    /**
     * The default URL of the publisher for the environment.
     */
    publisherUrl: string
    /**
     * The default URL of the aggregator for the environment.
     */
    aggregatorUrl: string
    /**
     * The default path of the data file for the environment.
     */
    payloadSourceFile: string,
    /**
     * URL to a redis instance that can be used to persist state across expiriments.
     */
    redisUrl?: RedisUrl,
}

/**
 * Default configurations for various running environments.
 */
const ENVIRONMENT_DEFAULTS: { [index: string]: EnvironmentConfig } = {
    "localhost": {
        "publisherUrl": "http://localhost:31415",
        "aggregatorUrl": "http://localhost:31415", // Run as daemon
        "payloadSourceFile": "../../../data.bin" // Within the k6 folder
    },
    "walrus-performance-network": {
        "publisherUrl": "http://walrus-publisher-0.walrus-publisher:31415",
        "aggregatorUrl": "http://walrus-aggregator-0.walrus-aggregator:31415",
        "payloadSourceFile": "/opt/k6/data/data.bin",
        "redisUrl": "redis://localhost:6379",
    },
    "walrus-testnet": {
        "publisherUrl": "https://publisher.walrus-testnet.walrus.space",
        "aggregatorUrl": "https://aggregator.walrus-testnet.walrus.space",
        "payloadSourceFile": "../../../data.bin", // Within the k6 folder
        "redisUrl": "redis://localhost:6379",
    }
}

const ENVIRONMENT = __ENV.ENVIRONMENT || "walrus-testnet";

/**
 * The URL of the publisher to use.
 */
export const PUBLISHER_URL: string = __ENV.PUBLISHER_URL
    || ENVIRONMENT_DEFAULTS[ENVIRONMENT].publisherUrl;

/**
 * The URL of the aggregator to use.
 */
export const AGGREGATOR_URL: string = __ENV.AGGREGATOR_URL
    || ENVIRONMENT_DEFAULTS[ENVIRONMENT].aggregatorUrl;

/**
 * The path to a file that is used as the source of all the blobs.
 * Each blob is taken as a slice from the raw bytes of the file.
 */
export const PAYLOAD_SOURCE_FILE: string = __ENV.PAYLOAD_SOURCE_FILE
    || ENVIRONMENT_DEFAULTS[ENVIRONMENT].payloadSourceFile;

/**
 * An optional URL of a redis instance in which to persist cross-experiment state,
 * in the form redis[s]://[[username][:password]@][host][:port][/db-number]
 */
export const REDIS_URL: RedisUrl | undefined = (__ENV.REDIS_URL != undefined)
    ? __ENV.REDIS_URL as RedisUrl
    : ENVIRONMENT_DEFAULTS[ENVIRONMENT].redisUrl;
