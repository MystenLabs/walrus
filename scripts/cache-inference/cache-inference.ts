// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

import yargs from 'yargs';
import { hideBin } from 'yargs/helpers';
import mdbookOperatorsJson from '../../docs/book/assets/operators.json';
import { AggregatorData, AggregatorDataVerbose, HeaderValue, Operators } from './types';

type HeaderMatch = {
    key: string;
    value: string | null
};

// Speedups bigger than this value (milliseconds) will are attributed to cache hits.
const THRESHOLD_MS: number = 1000;

// Filters header keys by searching for "cache" substring inside them.
function headerKeyContainsCache(headers: Headers): HeaderMatch[] {
    let filtered = [];
    headers.forEach((value, key, _parent) => {
        if (key.toLowerCase().includes("cache")) {
            filtered.push({key, value});
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
) {
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

        let headersWithCacheKey1 = headerKeyContainsCache(headers1);
        let headersWithCacheKey2 = headerKeyContainsCache(headers2);

        // Update value.cache in the existing operators
        if (headersHaveCacheHit(headersWithCacheKey1)) { // Try identifying cache-hit on the first request
            console.warn(`Error measuring cache speedup for ${blobUrl}:`);
            console.warn(`First fetch is a cache-hit: ${JSON.stringify(headersWithCacheKey1)}`);
            value.cache = true;
        } else {
            value.cache = speedupMs > THRESHOLD_MS || headersHaveCacheHit(headersWithCacheKey2)
        }
        if (verbose) {
            // Create a single key -> value1, value2 mapping
            // ie. commonkey -> [value from first fetch, value from second fetch]
            const map2 = Object.fromEntries(headersWithCacheKey2.map(({ key, value }) => [key, value]));
            const merged = headersWithCacheKey1.reduce<Record<string, [HeaderValue, HeaderValue]>>(
                (acc, { key, value }) => {
                    acc[key] = [value, map2[key] ?? undefined];
                    return acc;
                }, {}
            );

            aggregatorsVerbose[url].cacheHeaders = merged;
            aggregatorsVerbose[url].cacheSpeedupMs = [speedupMs, [fetch1, fetch2]];
        }
    }
}

// Get command line arguments
let parsedArgs: {
  mainnetBlobId: string;
  testnetBlobId: string;
  verbose: boolean;
} = {
  mainnetBlobId: '',
  testnetBlobId: '',
  verbose: false
};

yargs(hideBin(process.argv))
  .scriptName('bin')
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
        }),
    (argv) => {
      parsedArgs = {
        mainnetBlobId: argv.mainnetBlobId!,
        testnetBlobId: argv.testnetBlobId ?? argv.mainnetBlobId!,
        verbose: argv.verbose ?? false,
      };
    }
  )
  .strict()
  .parseSync()

async function run() {
    const { mainnetBlobId, testnetBlobId, verbose } = parsedArgs;
    const nodes: Operators = mdbookOperatorsJson;
    await updateAggregatorCacheInfo(nodes.mainnet.aggregators, mainnetBlobId, verbose);
    await updateAggregatorCacheInfo(nodes.testnet.aggregators, testnetBlobId, verbose);
    console.log(JSON.stringify(nodes, null, 2))
}

// Run for both networks
run()
