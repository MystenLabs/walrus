> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

This quickstart shows the core blob operations, store, read, and check status, three ways: the Walrus CLI, the HTTP API, and Python. Each operation appears in all three so you can pick the surface that fits your stack, or compare them side by side.

Walrus does not ship a dedicated Python SDK. A Python backend integrates with Walrus through the HTTP API, reading from an aggregator and writing to a publisher, or by driving the `walrus` CLI as a subprocess. This page uses the HTTP API for the Python examples because it needs nothing beyond the `requests` library.

- [x] No API key required. The public Walrus aggregator and publisher endpoints are open.
- [x] Set the endpoints you want to use. The examples below use the Testnet endpoints from the [Network Reference](/docs/network-reference#aggregators-and-publishers):

```sh
export AGGREGATOR=https://aggregator.walrus-testnet.walrus.space
export PUBLISHER=https://publisher.walrus-testnet.walrus.space
```

- [x] Choose a surface and complete the necessary setup:
  - **CLI:** Install and configure the `walrus` client. See [Getting Started](/docs/getting-started).
  - **HTTP API:** Any HTTP client, such as `curl`.
  - **Python:** Install the requests library with `pip install requests`.

> **Public endpoint limits**
>
> Public aggregators and publishers limit requests to 10 MiB, and Walrus does not provide a public unauthenticated publisher on Mainnet. For larger blobs or production Mainnet writes, run your own publisher, use an upload relay, or drive the CLI. See the [Mainnet Publisher Production Guide](/docs/operator-guide/publishers/mainnet-production-guide).
## Store a blob

Store a file for a number of epochs. The store returns a blob ID.

```sh
$ walrus store ./report.pdf --epochs 5
```

## Read a blob

Read a blob using its blob ID.

```sh
$ walrus read <BLOB_ID> --out ./report.pdf
```

Without the flag `--out`, the CLI writes the blob to standard output.

When you read a blob immediately after certifying it, a public aggregator can briefly return `404` before its read path catches up. Retry with backoff in that window, as the [full Python example](#full-python-example) shows. See [Reading blobs after upload](/docs/troubleshooting/reading-blobs-after-upload) for more.

## Check a blob's status

Check whether a blob is available and its registered onchain information.

```sh
$ walrus blob-status --blob-id <BLOB_ID>
```

The CLI reports the blob's certification status and end epoch from its Sui object. See [Reading blobs](/docs/walrus-client/reading-blobs) to learn more about certification status and Sui objects.

## Full Python example

The following script stores a blob, reads it back, and verifies the round trip against a public Testnet publisher and aggregator.

```python
import os
import time

import requests

AGGREGATOR = "https://aggregator.walrus-testnet.walrus.space"
PUBLISHER = "https://publisher.walrus-testnet.walrus.space"

def store_blob(data: bytes, epochs: int = 5) -> str:
    response = requests.put(
        f"{PUBLISHER}/v1/blobs", params={"epochs": epochs}, data=data
    )
    response.raise_for_status()
    result = response.json()
    if "newlyCreated" in result:
        return result["newlyCreated"]["blobObject"]["blobId"]
    return result["alreadyCertified"]["blobId"]

def read_blob(blob_id: str, attempts: int = 5, backoff: float = 1.0) -> bytes:
    # A public aggregator can briefly return 404 or a 5xx right after a blob is
    # certified, before its read path catches up. Because you know the blob was
    # just stored, retry those statuses with backoff before giving up.
    response = None
    for attempt in range(attempts):
        response = requests.get(f"{AGGREGATOR}/v1/blobs/{blob_id}")
        if response.status_code == 200:
            return response.content
        retryable = response.status_code == 404 or 500 <= response.status_code < 600
        if not retryable:
            break
        time.sleep(backoff * (2**attempt))
    response.raise_for_status()
    return response.content

if __name__ == "__main__":
    original = os.urandom(1024)
    blob_id = store_blob(original)
    print(f"stored blob {blob_id}")

    downloaded = read_blob(blob_id)
    assert downloaded == original, "round trip mismatch"
    print(f"read {len(downloaded)} bytes back, round trip verified")
```

For more Python examples, including driving the CLI through its JSON interface and tracking Walrus events, see [Using Walrus with Python](/docs/examples/python).

## References

- Store blobs with the CLI: [Store blobs with the Walrus client](/docs/walrus-client/storing-blobs)
- Use the HTTP API in depth: [Storing blobs with the HTTP API](/docs/http-api/storing-blobs) and [Reading blobs with the HTTP API](/docs/http-api/reading-blobs)
- Run a publisher that pays for storage on behalf of your users: [Mainnet Publisher Production Guide](/docs/operator-guide/publishers/mainnet-production-guide)
- Canonical endpoints and IDs: [Network Reference](/docs/network-reference)