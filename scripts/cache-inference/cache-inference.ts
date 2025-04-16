// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

import mdbookOperatorsJson from '../../docs/book/assets/operators.json';
import { AggregatorData, AggregatorDataVerbose, HeaderValue, Operators } from './types';

const KnownCacheKeys = [
    "cdn-cache",
    "cdn-cachedat",
    "cache-status",
    "cache-control",
    "cf-cache-status",
    "x-cache-status"
];

type HeaderMatch = {
    key: string;
    value: string | null
};
type HasCacheOutput = {
    hasCache: boolean;
    matches: HeaderMatch[];
};

function headerKeyContainsCache(headers: Headers): HasCacheOutput {
    const matches = [...headers.entries()].filter(([key, _]) => {
        return key.toLowerCase().includes("cache")
    }).map(([key, value]) => { return { key, value } });

    return {
        hasCache: matches.length > 0,
        matches
    };
}

function headersHaveCacheHit(matches: HeaderMatch[]): boolean {
    return matches.some(({ key: _, value }) => {
        return value?.toLowerCase().includes("hit");
    });
}

async function updateAggregatorCacheInfo(
    aggregators: Record<string, AggregatorData>,
    blobId: string,
    threshold: number,
) {

    // Used for debugging purposes
    const aggregatorsVerbose: Record<string, AggregatorDataVerbose> = {};
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
            console.error(`Error during measuring ${blobUrl}:`);
            console.error(e);
            continue;
        }
        const speedupMs = fetch1 - fetch2;

        let output1 = headerKeyContainsCache(headers1);
        let output2 = headerKeyContainsCache(headers2);
        const matches1 = output1.matches;
        const matches2 = output2.matches;
        // Update value.cache in the existing operators
        if (headersHaveCacheHit(matches1)) { // Try identifying cache-hit on the first request
            console.warn(`Error measuring cache speedup for ${blobUrl}:`);
            console.warn(`First fetch is a cache-hit: ${JSON.stringify(matches1)}`);
            value.cache = { hasCache: true, speedupMs: value.cache?.speedupMs };
        } else {
            value.cache = {
                hasCache: speedupMs > threshold || headersHaveCacheHit(matches2),
                speedupMs
            };
        }
        const hasCache = value.cache.hasCache;
        // Create a single key -> value1, value2 mapping
        const map2 = Object.fromEntries(matches2.map(({ key, value }) => [key, value]));
        const merged = matches1.reduce<Record<string, [HeaderValue, HeaderValue]>>(
            (acc, { key, value }) => {
                acc[key] = [value, map2[key] ?? undefined];
                return acc;
            }, {}
        );

        const missing = Object.keys(merged).filter((key) => !KnownCacheKeys.includes(key));

        if (missing.length > 0) {
            console.warn(`New '.*cache.*' headers found:`);
            missing.map((missing) => {
                console.warn(`- ${missing}: ${merged[missing]}`);
            });
        }

        aggregatorsVerbose[url] = {
            cache: {
                hasCache,
                headers: merged,
                speedupMs: [speedupMs, [fetch1, fetch2]]
            }
        };
    }
    // let results = {
    //     aggregators: aggregatorsVerbose,
    // }
    // console.log(JSON.stringify(results, null, 2));
}

const THRESHOLD: number = 1000;

// Get command line arguments
const args = process.argv.slice(2);
if (args.length !== 2) {
    console.error('Usage: ts-node cache-inference.ts <mainnet-blob-id> <testnet-blob-id>');
    process.exit(1);
}

const [BLOB_ID_MAINNET, BLOB_ID_TESTNET] = args;

async function run() {
    const nodes: Operators = mdbookOperatorsJson;
    await updateAggregatorCacheInfo(nodes.mainnet.aggregators, BLOB_ID_MAINNET, THRESHOLD);
    await updateAggregatorCacheInfo(nodes.testnet.aggregators, BLOB_ID_TESTNET, THRESHOLD);
    console.log(JSON.stringify(nodes, null, 2))
}

// Run for both networks
run()
