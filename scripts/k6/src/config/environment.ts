// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

import { loadParameters } from "../lib/utils.ts"

/**
 * Type alias for a redis URL.
 */
type RedisUrl = `redis://${string}`

/**
 * A configuration for a test environment.
 */
export interface EnvironmentConfig {
    /**
     * The name of the environment.
     */
    environment: string,
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
     * URL to a redis instance that can be used to persist state across experiments.
     */
    redisUrl?: RedisUrl,
}

const ciEnvironmentDefaults: Omit<EnvironmentConfig, "environment"> = {
    "publisherUrl": "http://walrus-publisher-0.walrus-publisher:31415",
    "aggregatorUrl": "http://walrus-aggregator-0.walrus-aggregator:31415",
    "payloadSourceFile": "/opt/k6/data/data.bin",
    "redisUrl": "redis://localhost:6379",
}

/**
 * Default configurations for various running environments.
 */
const ENVIRONMENT_DEFAULTS: { [index: string]: Omit<EnvironmentConfig, "environment"> } = {
    "localhost": {
        "publisherUrl": "http://localhost:31415",
        "aggregatorUrl": "http://localhost:31415", // Run as daemon
        "payloadSourceFile": "../../../data.bin", // Within the k6 folder
        "redisUrl": undefined // Set to undefined as the key is used to fetch the ENV variable.
    },
    "walrus-testnet": {
        "publisherUrl": "https://publisher.walrus-testnet.walrus.space",
        "aggregatorUrl": "https://aggregator.walrus-testnet.walrus.space",
        "payloadSourceFile": "../../../data.bin", // Within the k6 folder
        "redisUrl": undefined // Set to undefined as the key is used to fetch the ENV variable.
    },
    "ci-testnet-performance": ciEnvironmentDefaults,
    "performance-main-baseline": ciEnvironmentDefaults,
    "performance-main-latency": ciEnvironmentDefaults,
}

/**
 * The default environment used if none is specified.
 */
export const DEFAULT_ENVIRONMENT: string = "walrus-testnet";

/**
 * Load the defaults for the environment specified by `environment` and update
 * the defaults with any arguments set on the command line.
 */
export function loadEnvironment(): EnvironmentConfig {
    const environmentName = __ENV["WALRUS_K6_ENVIRONMENT"] || DEFAULT_ENVIRONMENT;
    const environment = loadParameters<Omit<EnvironmentConfig, "environment">>({
        ...ENVIRONMENT_DEFAULTS[environmentName]
    });
    return {
        environment: environmentName,
        ...environment
    }
}
