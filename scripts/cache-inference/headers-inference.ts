
import mdbookOperatorsJson from '../../docs/book/assets/operators.json';
import { Network, Operators } from './types';

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

async function run(network: Network, blobId: string) {
    const nodes: Operators = mdbookOperatorsJson;
    const aggregators = nodes[network].aggregators;

    if (!aggregators) {
        console.error(`Expected ${network} aggregators`);
        throw new Error(`Expected ${network} aggregators`);
    }

    for (const [url, value] of Object.entries(aggregators)) {
        const blobUrl = new URL(`v1/blobs/${blobId}`, url);
        let resp: Response;
        try {
            resp = await fetch(blobUrl);
        } catch (e) {
            console.error(`Error fetching ${blobUrl}:`);
            console.error(e);
            continue;
        }

        let headers = resp.headers;
        if (!value.cache) {
            value.cache = { hasCache: false, headers: [] };
        }
        let output = headerKeyContainsCache(headers);
        value.cache.hasCache = output.hasCache;

        const matches = output.matches;
        const missing = matches.filter(({ key, value: _ }) => !KnownCacheKeys.includes(key));
        if (missing.length > 0) {
            console.warn(`New '.*cache.*' headers found:`);
            missing.map((missing) => {
                console.warn(`- ${missing.key}: ${missing.value}`);
            });
        }
        value.cache.headers = matches;
    }
    let results = {
        aggregators: aggregators,
    }
    console.log(JSON.stringify(results, null, 2));
}


// TMP
const BLOB_ID_MAINNET = "...blob id to test for mainnet...";
// const BLOB_ID_TESTNET = "...blob id to test for testnet...";

run("mainnet", BLOB_ID_MAINNET);
// run("testnet", BLOB_ID_TESTNET);
