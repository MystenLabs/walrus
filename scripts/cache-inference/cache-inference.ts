// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

import yargs from 'yargs';
import { hideBin } from 'yargs/helpers';
import mdbookOperatorsJson from '../../docs/book/assets/operators.json';
import { AggregatorData, HeaderValue, Operators } from './types';

type HeaderMatch = {
    key: string;
    value: string | null
};

// Speedups between first and second fetch that are bigger than this value (milliseconds) will be
// attributed to cache hits.
const CACHE_LATENCY_IMPROVEMENT_THRESHOLD_MS: number = 1000;
// Latencies smaller than this value (milliseconds) will be attributed to cache hits.
const HIT_LATENCY_THRESHOLD_MS: number = 800;

// Filters header keys by searching for "cache" substring inside them.
function headerKeyContainsCache(headers: Headers): HeaderMatch[] {
    const filtered = [];
    headers.forEach((value, key, _parent) => {
        if (key.toLowerCase().includes("cache")) {
            filtered.push({ key, value });
        }
    });
    return filtered;
}

function headersHaveCacheHit(matches: HeaderMatch[]): boolean {
    return matches.some(({ key: _, value }) => {
        return value?.toLowerCase().includes("hit");
    });
}

async function updateAggregatorCacheInfo(
    aggregators: Record<string, AggregatorData>,
    blobId: string,
    verbose: boolean,
    removeOnError: boolean,
) {
    const urlsToRemove: string[] = [];

    for (const [url, value] of Object.entries(aggregators)) {
        const blobUrl = new URL(`v1/blobs/${blobId}`, url);
        let fetch1: number;
        let fetch2: number;
        let headers1: Headers;
        let headers2: Headers;

        try {
            let start = Date.now();
            const resp1 = await fetch(blobUrl);
            // Measure the full response time (note though that we should use small blobs anyway
            // here).
            await resp1.arrayBuffer();
            fetch1 = Date.now() - start;
            headers1 = resp1.headers;
            start = Date.now();
            const resp2 = await fetch(blobUrl);
            await resp2.arrayBuffer();
            fetch2 = Date.now() - start
            headers2 = resp2.headers;
        } catch (e) {
            console.error(`${url}: Error while measuring aggregator performance:\n  ${e}`);
            urlsToRemove.push(url);
            value.functional = false;
            value.cache = undefined;
            continue;
        }
        const speedupMs = fetch1 - fetch2;

        const headersWithCacheKey1 = headerKeyContainsCache(headers1);
        const headersWithCacheKey2 = headerKeyContainsCache(headers2);

        // Update the existing operator.
        value.functional = undefined;
        value.cache = true;
        if (fetch1 < HIT_LATENCY_THRESHOLD_MS) {
            console.warn(
                `${url}: Latency of first fetch (${fetch1}ms) is smaller than the ` +
                `threshold ${HIT_LATENCY_THRESHOLD_MS}ms, indicating a cache hit.`
            );
        } else if (fetch2 < HIT_LATENCY_THRESHOLD_MS) {
            console.warn(
                `${url}: Latency of second fetch (${fetch2}ms) is smaller than the ` +
                `threshold ${HIT_LATENCY_THRESHOLD_MS}ms, indicating a cache hit.`
            );
        } else if (speedupMs > CACHE_LATENCY_IMPROVEMENT_THRESHOLD_MS) {
            console.warn(
                `${url}: Speedup (${speedupMs}ms) is greater than the threshold ` +
                `${CACHE_LATENCY_IMPROVEMENT_THRESHOLD_MS}ms`
            );
        } else if (headersHaveCacheHit(headersWithCacheKey1)) {
            console.warn(
                `${url}: Headers of first fetch indicate a cache-hit: ` +
                `${JSON.stringify(headersWithCacheKey1)}`
            );
        } else if (headersHaveCacheHit(headersWithCacheKey2)) {
            console.warn(
                `${url}: Headers of second fetch indicate a cache-hit: ` +
                `${JSON.stringify(headersWithCacheKey2)}`
            );
        } else {
            console.warn(
                `${url}: No cache-hit detected:\n` +
                `  first fetch: ${fetch1}ms, second fetch: ${fetch2}ms\n` +
                `  first fetch headers: ${JSON.stringify(headersWithCacheKey1)}\n` +
                `  second fetch headers: ${JSON.stringify(headersWithCacheKey2)}`
            );
            value.cache = false;
        }
        if (verbose) {
            // Create a mapping commonKey -> [value from first fetch, value from second fetch]
            const map2 = Object.fromEntries(
                headersWithCacheKey2.map(({ key, value }) => [key, value])
            );
            const merged = headersWithCacheKey1.reduce<Record<string, [HeaderValue, HeaderValue]>>(
                (acc, { key, value }) => {
                    acc[key] = [value, map2[key] ?? undefined];
                    return acc;
                }, {}
            );

            value.cacheHeaders = merged;
            value.cacheSpeedupMs = [speedupMs, [fetch1, fetch2]];
        }
    }

    // Remove aggregators that had errors
    if (removeOnError && urlsToRemove.length > 0) {
        console.warn(`Removing ${urlsToRemove.length} aggregators due to errors`);
        for (const url of urlsToRemove) {
            delete aggregators[url];
            console.warn(`Removed aggregator ${url}`);
        }
    }
}

// Get command line arguments
let parsedArgs: {
    mainnetBlobId: string;
    testnetBlobId: string;
    verbose: boolean;
    removeOnError: boolean;
} = {
    mainnetBlobId: '',
    testnetBlobId: '',
    verbose: false,
    removeOnError: false,
};

yargs(hideBin(process.argv))
    .scriptName('cache-inference')
    .command(
        '$0 <mainnetBlobId> [testnetBlobId]',
        'Run the binary',
        (yargs) =>
            yargs
                .positional('mainnetBlobId', {
                    describe: 'Blob ID for mainnet',
                    type: 'string',
                })
                .positional('testnetBlobId', {
                    describe: 'Blob ID for testnet (optional, defaults to mainnetBlobId)',
                    type: 'string',
                })
                .option('verbose', {
                    alias: 'v',
                    type: 'boolean',
                    description: 'Run with verbose logging',
                    default: false,
                })
                .option('remove-on-error', {
                    alias: 'r',
                    type: 'boolean',
                    description: 'Remove operators on error',
                    default: false,
                }),
        (argv) => {
            parsedArgs = {
                mainnetBlobId: argv.mainnetBlobId!,
                testnetBlobId: argv.testnetBlobId ?? argv.mainnetBlobId!,
                verbose: argv.verbose ?? false,
                removeOnError: argv.removeOnError ?? false,
            };
        }
    )
    .strict()
    .parseSync()

async function run() {
    const { mainnetBlobId, testnetBlobId, verbose, removeOnError } = parsedArgs;
    const nodes: Operators = mdbookOperatorsJson;
    await updateAggregatorCacheInfo(
        nodes.mainnet.aggregators,
        mainnetBlobId,
        verbose,
        removeOnError,
    );
    await updateAggregatorCacheInfo(
        nodes.testnet.aggregators,
        testnetBlobId,
        verbose,
        removeOnError,
    );
    console.log(JSON.stringify(nodes, null, 2))
}

// Run for both networks
run()
