# Cache Inference Script

Detects and updates cache configuration for Walrus aggregators by analyzing response headers and request timing.

## Overview

The script makes two sequential requests to each aggregator and determines caching status through:
1. **Headers**: Detects cache-related headers with "hit" values
2. **Performance**: Identifies caching through response time improvements (>1000ms)

## Usage

```bash
npm install --prefix scripts/cache-inference
npx ts-node ./cache-inference.ts <mainnetBlobId> [testnetBlobId] [--verbose]
```

### Arguments

- `mainnetBlobId`: Blob ID for mainnet testing
- `testnetBlobId`: (Optional) Blob ID for testnet testing
- `--verbose`: Enable detailed logging

## How It Works

1. Reads aggregator URLs from `docs/book/assets/operators.json`
2. For each aggregator:
   - Makes two requests to `v1/blobs/{blobId}`
   - Checks headers and timing
   - Updates cache status

## Output

JSON containing updated cache information for all aggregators. With `--verbose`, includes:
- Cache headers
- Response times with speedup
