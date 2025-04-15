
import mdbookOperatorsJson from '../../docs/book/assets/operators.json';
import { AggregatorDataVerbose, HeaderValue, Network, Operators } from './types';

const KnownCacheKeys = [
    "cdn-cache",
    "cdn-cachedat",
    "cache-status",
    "cache-control",
    "cf-cache-status",
    "x-cache-status"
];

type HasCacheOutput = {
    hasCache: boolean;
    matches: { key: string, value: string | null }[];
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

async function run(network: Network, blobId: string, threshold: number) {
    const nodes: Operators = mdbookOperatorsJson;
    const aggregators = nodes[network].aggregators;

    if (!aggregators) {
        console.error(`Expected ${network} aggregators`);
        throw new Error(`Expected ${network} aggregators`);
    }

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
        const hasCache = speedupMs > threshold;
        value.cache = { hasCache, speedupMs }
        const matches1 = output1.matches;
        const matches2 = output2.matches;

        // Create a single key -> value1, value2 mapping
        const map2 = Object.fromEntries(matches2.map(({ key, value }) => [key, value]));
        const merged = matches1.reduce<Record<string, [HeaderValue, HeaderValue]>>((acc, { key, value }) => {
            acc[key] = [value, map2[key] ?? undefined];
            return acc;
        }, {});

        const missing = Object.keys(merged).filter((key) => !KnownCacheKeys.includes(key));

        if (missing.length > 0) {
            console.warn(`New '.*cache.*' headers found:`);
            missing.map((missing) => {
                console.warn(`- ${missing}: ${merged[missing]}`);
            });
        }

        aggregatorsVerbose[url] = { cache: { hasCache, headers: merged, speedupMs: [speedupMs, [fetch1, fetch2]] } };
    }
    let results = {
        aggregators: aggregatorsVerbose,
    }
    console.log(JSON.stringify(results, null, 2));
}


const THRESHOLD: number = 1000;
// TMP
// const BLOB_ID_MAINNET = "...blob id to test for mainnet...";
const BLOB_ID_TESTNET = "ieK_SW6tkzrJn4kATNDLhpS8-AX4CrzinrzGdEt4Gzk";

// run("mainnet", BLOB_ID_MAINNET);
run("testnet", BLOB_ID_TESTNET, THRESHOLD);
