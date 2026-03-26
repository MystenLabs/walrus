This page covers common errors you might encounter when using Walrus. Each entry includes the error message, its cause, and how to troubleshoot it.

## Configuration errors

Errors in this section occur when the Walrus CLI cannot load or parse its configuration.

#### `could not find a valid Walrus configuration file`

**Cause:** The CLI cannot find a configuration file in any of the default locations.

**Solution:** Create the configuration directory and download a fresh configuration file:

```
$ mkdir -p ~/.config/walrus
$ curl https://docs.wal.app/setup/client_config.yaml -o ~/.config/walrus/client_config.yaml
```

If your configuration file is stored in a non-default location, you can specify a custom path:

```
$ walrus info --config /path/to/client_config.yaml
```

#### `unable to parse the client config file`

**Cause:** The configuration file has invalid YAML syntax, is missing required fields such as `system_object` or `staking_object`, or contains typos in field names.

**Solution:** Validate the YAML syntax and verify that all required fields are present.

Alternatively, download a fresh configuration file:

```
$ curl https://docs.wal.app/setup/client_config.yaml -o ~/.config/walrus/client_config.yaml
```

If you download a fresh file, be aware that any custom configuration may be overwritten or lost. Take precaution to preserve necessary configuration parameters. 

#### `the specified Walrus system object does not exist`

**Cause:** The configuration points to a Walrus system object that does not exist on the current Sui network. This happens when you use an outdated configuration, or you target the wrong network.

**Solution:** Download the latest configuration for your target network. Verify that your Sui wallet is configured for the correct network (Mainnet or Testnet).

#### `Sui RPC url is not specified as a CLI argument or in the client configuration, and no valid Sui wallet was provided`

**Cause:** The CLI cannot determine which Sui RPC endpoint to use. No RPC URL was provided through `--rpc-url`, the configuration file does not contain RPC URLs, and no valid wallet was found.

**Solution:** Specify an RPC URL directly:

```
$ walrus info --rpc-url https://fullnode.mainnet.sui.io:443
```

Alternatively, add RPC URLs to your configuration file or verify that your Sui wallet is properly configured.

#### `cannot connect to Sui RPC nodes at URLS`

**Cause:** The CLI cannot establish a connection to any of the specified Sui RPC endpoints.

**Solution:** Check your internet connection and verify the RPC URLs are correct. Try a different RPC endpoint:

```
$ walrus info --rpc-url https://fullnode.mainnet.sui.io:443
```

---

## Storage duration errors

Errors in this section occur when specifying how long to store a blob.

#### `either epochs or earliest_expiry_time or end_epoch must be provided`

**Cause:** No storage duration was specified when storing a blob.

**Solution:** Provide one duration option when storing:

```
$ walrus store file.txt --epochs 5
```

You can also use `--earliest-expiry-time` or `--end-epoch` instead.

#### `exactly one of epochs, earliest-expiry-time, or end-epoch must be specified`

**Cause:** Multiple storage duration options were provided. Only one is allowed per command.

**Solution:** Provide exactly one of `--epochs`, `--earliest-expiry-time`, or `--end-epoch`.

#### `invalid epoch count; please a number >0 or max`

**Cause:** An invalid value was provided for the `--epochs` flag.

**Solution:** Provide a positive integer or `max` for the `--epochs` flag.

#### `blobs can only be stored for up to MAX_EPOCHS epochs ahead`

**Cause:** The requested storage duration exceeds the maximum allowed. The current maximum is 53 epochs.

**Solution:** Reduce the number of epochs, or use `--epochs max` to store for the maximum allowed duration. Check the current maximum with `walrus info`.

#### `expiry time is too far in the future`

**Cause:** The specified `--earliest-expiry-time` value exceeds the maximum allowed duration.

**Solution:** Use a shorter duration. Check the maximum epochs allowed with `walrus info`.

#### `end_epoch must be greater than the current epoch`

**Cause:** The `--end-epoch` value is not greater than the current epoch.

**Solution:** Provide an `--end-epoch` value that is greater than the current epoch. Check the current epoch with `walrus info epoch`.

#### `earliest_expiry_time must be greater than the current epoch start time and the current time`

**Cause:** The `--earliest-expiry-time` value is in the past.

**Solution:** Provide a value that is in the future relative to both the current epoch start time and the current wall-clock time.

## Input and validation errors

Errors in this section occur when command arguments are missing or invalid.

#### `either the file or blob ID must be defined`

**Cause:** A command requiring a blob identifier was called without providing one.

**Solution:** Provide either `--file` or `--blob-id`.

#### `no files, blob IDs, or object IDs specified`

**Cause:** A command such as `delete` or `burn-blobs` was called without providing any identifiers.

**Solution:** Provide at least one of `--files`, `--blob-id`, or `--object-id`.

#### `deletable blobs cannot be shared`

**Cause:** You attempted to share a deletable blob. Only permanent blobs can be shared.

**Solution:** Create the blob with `--permanent` if you intend to share it, or convert it to permanent before sharing.

#### `exactly one of objectIds, all, or allExpired must be specified`

**Cause:** Multiple or no selection options were provided to `burn-blobs`.

**Solution:** Provide exactly one of `--object-ids`, `--all`, or `--all-expired`.

#### `cannot provide both paths and blob_inputs`

**Cause:** Both `--paths` and `--blobs` options were provided to `store-quilt`.

**Solution:** Use either `--paths` or `--blobs`, not both.

#### `either paths or blob_inputs must be provided`

**Cause:** No input was provided to `store-quilt`.

**Solution:** Provide either `--paths` or `--blobs`.

#### `exactly one of address or object must be set`

**Cause:** A command requires either an address or object ID, but both or neither were provided.

**Solution:** Provide exactly one of the required options.

#### `exactly one of nodeId, nodeUrl, committee, or activeSet must be specified`

**Cause:** Multiple or no node selection options were provided to a command such as `health`.

**Solution:** Provide exactly one of `--node-id`, `--node-url`, `--committee`, or `--active-set`.

#### `node URL URL not found in active set`

**Cause:** The provided node URL does not match any node in the active set.

**Solution:** Verify the node URL is correct, or use `--node-id` instead.

#### Quilt query pattern errors

**Error:**

```
Exactly one query pattern must be specified. Valid query patterns are:
- quiltId + identifiers
- quiltId + tag
- quiltPatchIds
- quiltId only
```

**Cause:** An invalid combination of query options was provided to `read-quilt` or `list-patches-in-quilt`.

**Solution:** Use exactly one of the valid query patterns:

- `--quilt-id` with `--identifiers`
- `--quilt-id` with `--tag`
- `--quilt-patch-ids` alone
- `--quilt-id` alone

#### `unrecognised trace exporter VALUE`

**Cause:** An invalid value was provided for the `--trace-cli` flag.

**Solution:** Use either `--trace-cli otlp` or `--trace-cli file=/path/to/file`.

#### `The object ID of an exchange object must be specified`

**Cause:** The `get-wal` command requires an exchange object ID, but none was provided. This command is only available on Testnet.

**Solution:** Specify the exchange object ID:

```
$ walrus get-wal --exchange-id EXCHANGE_OBJECT_ID
```

#### `operation cancelled by user`

**Cause:** You declined a confirmation prompt, for example the upload relay tip confirmation.

**Solution:** Re-run the command and confirm when prompted, or use `--skip-tip-confirmation` for upload relay to bypass the prompt.

## Blob ID errors

Errors in this section occur when a blob ID is missing, malformed, or refers to a blob that is unavailable.

#### `you seem to be using a numeric value in decimal format corresponding to a Walrus blob ID`

**Cause:** A decimal blob ID (often copied from a Sui explorer) was provided instead of the URL-safe base64 format that Walrus uses.

**Solution:** The error message includes the correct blob ID. You can also convert it manually:

```
$ walrus convert-blob-id DECIMAL_VALUE
```

:::info
Some on-chain representations and Sui explorers display blob IDs as large decimal numbers. These are not valid blob IDs for Walrus API calls. Always convert them to URL-safe base64 format before use.
:::

#### `the provided blob ID is invalid`

**Cause:** The provided blob ID string cannot be parsed as either a base64-encoded blob ID or a decimal value.

**Solution:** Verify the blob ID is correct and is in URL-safe base64 format (43 characters). If you copied it from a Sui explorer, convert it with `walrus convert-blob-id`.

#### Blob not found

**Error:**

```
Error: Blob not found on Sui
HTTP 404: Blob ID not registered
```

**Cause:** The blob was never uploaded, has expired (past its storage epoch), belongs to a different network, or was marked invalid on-chain.

**Solution:** Verify the blob ID is correct and that you are querying the correct network. Check the blob status:

```
$ walrus blob-status --blob-id BLOB_ID
```

If the blob expired, re-upload it. On Mainnet, each storage epoch is approximately 14 days. The maximum storage duration is 53 epochs.

#### `the blob has not been registered or has already expired`

**Cause:** The blob was registered on-chain but slivers were never distributed to storage nodes, or several storage nodes are lagging behind in processing network events during periods of heavy uploads.

**Solution:**

Retry the upload. If the error persists during high network activity, wait a few minutes and try again. Storage nodes might need time to process recent events.

## Insufficient funds

#### `Insufficient SUI for gas` or `Insufficient WAL for storage fees`

**Error (variants):**

```
Error: could not find SUI coins with sufficient balance [requested_amount=Some(1000000000)]
Error: could not find WAL coins with sufficient balance
ClientError { kind: NoCompatiblePaymentCoin }
```

**Cause:** Your wallet does not have enough SUI for gas fees or enough WAL for storage payments.

**Solution:** Check your wallet balance:

```
$ sui client gas
```

Add SUI or WAL to your wallet as needed. Use `walrus info` to check current storage costs per epoch. If you are using a publisher daemon, verify that the daemon wallet has sufficient funds for both SUI gas and WAL storage payments.

#### `there is enough balance to cover the requested amount but cannot be achieved with less than the maximum number of coins allowed`

**Cause:** Your balance is sufficient but spread across too many coin objects.

**Solution:** Merge your coins in the wallet and retry the operation.

## Transaction errors

#### Transaction timeout

**Error:**

```
Error: Transaction not confirmed within timeout
Warning: Transaction pending...
Error: RPC timeout waiting for transaction
```

**Cause:** Network congestion on Sui, RPC node issues, or the transaction is stuck in the mempool.

**Solution:** Check whether the transaction succeeded on-chain before retrying:

```
$ sui client tx-block TRANSACTION_ID
```

If the transaction succeeded, proceed normally. If it failed or was not found, retry the operation.

:::caution
Do not assume a timed-out transaction failed. Always check the on-chain state first. Retrying a successful transaction can result in duplicate blob registrations and wasted funds.
:::

#### Transaction conflict

**Error:**

```
Error: Transaction rejected: Invalid nonce
Error: Conflicting transaction
```

**Cause:** Multiple transactions from the same wallet were submitted simultaneously, or the wallet state is out of sync with the network.

**Solution:** Serialize transactions from the same wallet. Wait for each transaction to confirm before submitting the next one. If you are using a publisher, it should queue transactions internally to avoid conflicts.

---

## Connectivity errors

Errors in this section occur when a client, publisher, or aggregator cannot reach other components in the network.

### Client cannot reach a publisher or aggregator

**Error:**

```
Error: Connection timeout
Error: Connection refused
Error: DNS resolution failed
```

**Cause:** The publisher or aggregator is down, a firewall is blocking the connection, or DNS is misconfigured.

**Solution:** Implement retry logic with exponential backoff in your application. If multiple publishers or aggregators are available, try an alternative endpoint. Set reasonable timeouts: 30 seconds for uploads and 10 seconds for reads.

See [Error Handling](/docs/troubleshooting/error-handling) for an example of retry logic in the TypeScript SDK.

#### Publisher cannot reach storage nodes

**Error:**

```
Error: Failed to distribute slivers
Warning: Storage node X unreachable
Error: Quorum not reached (received 1/2 of signatures)
```

**Cause:** One or more storage nodes are offline, overloaded, or unreachable due to a network partition.

**Solution:** Retry the upload. The publisher might reach different nodes on subsequent attempts. If 2/3 of storage nodes respond, the upload succeeds. If fewer than 2/3 respond, the upload fails. Check on-chain state to verify whether the blob was registered, then retry if necessary.

#### Aggregator cannot fetch enough slivers

**Error:**

```
Error: Failed to fetch slivers
Warning: Storage node Y unreachable
Error: Insufficient slivers
```

**Cause:** Storage nodes are offline, overloaded, or rate-limiting requests.

**Solution:** Retry the read request. Only 1/3 of primary slivers are needed to reconstruct a blob, so the system tolerates many nodes being offline. Try a different aggregator if available, or wait and retry.

---

## Storage node errors

Errors in this section relate to storage node data integrity.

#### Sliver not found

**Error:**

```
Error: Sliver not found on storage node
HTTP 404: Sliver ID not in database
```

**Cause:** The storage node lost data due to database corruption, disk failure, or a migration issue during an epoch transition.

**Solution:** Retry the read. Other nodes hold copies of the slivers through erasure coding. If fewer than 1/3 of nodes lost data, retrieval succeeds normally. If the error persists, check the on-chain blob status with `walrus blob-status`. Re-upload the blob if necessary.

#### Sliver hash mismatch

**Error:**

```
Error: Sliver hash mismatch
Error: Consistency check failed
```

**Cause:** Disk or memory corruption on the storage node, a Byzantine (malicious) node, or a network transmission error.

**Solution:** The aggregator automatically detects hash mismatches through Merkle tree verification and fetches replacement slivers from other nodes. No client-side action is required unless the error persists, in which case you can run a strict consistency check:

```
$ walrus read BLOB_ID --strict-consistency-check
```

#### Invalid signature from a storage node

**Error:**

```
Warning: Node X returned invalid signature
Error: Certificate verification failed
```

**Cause:** A compromised or malicious storage node, or a software bug in the node implementation.

**Solution:** The system handles this automatically. Walrus provides Byzantine Fault Tolerance (BFT) and functions correctly with up to 1/3 of nodes behaving maliciously. No client-side action is required.

## Encoding errors

#### Out of memory during encoding

**Error:**

```
Error: Out of memory
Error: Failed to allocate encoding buffer
```

**Cause:** The blob is too large for available memory. The encoding process requires approximately 2 to 3 times the blob size in RAM.

**Solution:** Run the CLI on a machine with sufficient RAM. Split large blobs into smaller chunks in your application before uploading. If you are using a publisher, use one with more available memory.