# Cache Inference Script

Detects and updates cache configuration for Walrus aggregators by analyzing response headers and
request timing.

## Overview

The script makes two sequential requests to each aggregator and determines caching status through
three cases:

1. **Performance**: The latency of an individual request is below some threshold (500ms).
1. **Performance improvement**: The latency of the second request is significantly lower (>1000ms)
   than the first request.
1. **Headers**: Detects cache-related headers with "hit" values.

## Usage

```bash
npm install --prefix scripts/cache-inference
npx ts-node scripts/cache-inference/cache-inference.ts <mainnetBlobId> [testnetBlobId] [--verbose] [--remove-on-error]
```

### Arguments

- `mainnetBlobId`: Blob ID for Mainnet testing
- `testnetBlobId`: (Optional) Blob ID for Testnet testing; if not set, the `mainnetBlobId` is used
- `--verbose`: Enable detailed logging
- `--remove-on-error`: Remove aggregators when receiving errors

## How It Works

1. Reads aggregator URLs from `docs/site/static/operators.json`
1. For each aggregator:
   - Makes two requests to `v1/blobs/{blobId}`
   - Checks headers and timing
   - Updates cache status

## Output

JSON containing updated cache information for all aggregators. With `--verbose`, this includes:

- Cache headers
- Response times with speedup
