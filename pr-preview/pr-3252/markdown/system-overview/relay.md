An upload relay is a server-side service that simplifies the upload process by handling the complexity of writing to multiple storage nodes. It provides batching capabilities and improved reliability for client applications.

Instead of your application making many requests to different storage nodes, you make a single request to the relay, which handles all node communications and returns a single certificate. Without a relay, uploading a blob requires one request per storage node (potentially 10 or more requests), managing parallel uploads, handling partial failures, and collecting confirmations from multiple nodes.

## Automatic retry logic

The relay implements server-side retry logic, including automatic retries for failed node connections, timeout handling for slow nodes, quorum management to ensure enough nodes store the data, and error recovery without client intervention.

When the relay receives a blob, it reuses the same uploader that the Walrus TypeScript SDK client uses for direct uploads, so the server inherits all quorum tracking and retry behavior. Within that helper, the SDK concurrently writes to each node, counts failures by shard weight, and keeps retrying until quorum is satisfied (or aborts with `NotEnoughBlobConfirmationsError` if it cannot reach quorum).

## Network optimization

The relay reduces client load by requiring only one connection, optimizing data transfer bandwidth, and handling network issues server-side.

The relay also provides server-side benefits, including persistent connections to storage nodes, connection pooling for efficiency, geographic routing to nearby nodes, and load balancing across multiple relay instances.

## Tip mechanism

Public or community relays often charge a tip to offset bandwidth, compute, and hosting costs. When a tip is required, the relay exposes the endpoint `/v1/tip-config` to be queried for information on relay's cost:

- **Free service:** `tip_config` is `no_tip`. Clients only pay onchain storage fees.

- **Paid service:** `tip_config` advertises how much the relay expects before it forwards your blob.

Your client must provide the `nonce` and `tx_id` from the payment transaction. Without this payment, the relay rejects the upload request.

Paid relays only accept uploads that include the advertised tip, so your request is prioritized and not throttled once the payment is verified. The payment transaction encodes `blob_digest`, `nonce`, and `unencoded_length`. The relay checks the provided `tx_id` and `nonce` pair before streaming data, preventing freeloading or replay attempts.

## When to use the relay

Web apps running in unprivileged browsers benefit the most because the relay removes the need to maintain dozens of outbound connections. Instead of orchestrating per-node uploads in the browser, the relay exposes a single POST to `/v1/blob-upload-relay` that handles encoding, sliver uploads, and certificate collection on your behalf.

The same constraints apply on low-powered devices. Mobile devices and low-powered laptops as the primary motivation for the relay, including clients with limited bandwidth or networking capability, such as mobile apps or browsers.

#### High-volume uploads

Backend services that need to upload many files in sequence can centralize the work through a relay. After a single POST, the relay performs encoding, sliver uploads, and confirmation aggregation before returning a certificate. 

#### Simplified error handling

Direct uploads keep the entire retry and quorum loop inside your process. Because failures collapse into one response, app code can treat uploads like any other POST and log a single error message instead of stitching together node-level incidents. That makes the relay the right choice whenever reducing error-handling code is a priority.

## When not to use the relay

For server-side applications, direct node communication offers full control over the upload process, the ability to implement custom retry and error handling, with no relay tip costs. Server-side batch processing benefits from direct control over custom batching strategies and server resource allocation.