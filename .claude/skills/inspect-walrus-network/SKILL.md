---
name: inspect-walrus-network
description: This skill should be used when the user asks to "inspect walrus network",
"check walrus network status", "query walrus storage nodes", "walrus committee info",
"walrus node health", "inspect-walrus-network", or discusses Walrus production network monitoring
for mainnet or testnet.
user_invocable: true
arguments:
  - name: network
    description: The network to inspect (testnet or mainnet)
    required: true
---

# Inspect Walrus Network

This skill inspects the Walrus production network (mainnet or testnet), retrieves committee and
storage node information, and answers queries about individual nodes or global network status.

## Prerequisites

The `walrus` CLI binary must be installed. If it is not available on the system, install it by
following the instructions at https://docs.wal.app/docs/getting-started. The typical install
command is:

```bash
curl -sSf https://docs.wal.app/setup/walrus-install.sh | sh -s -- -n testnet
```

After installation, the binary is typically at `~/.local/bin/walrus`. Verify with:

```bash
walrus --version
```

If `walrus` is not in PATH, check `~/.local/bin/walrus` or `~/.walrus/bin/walrus`.

## Configuration

Since this skill must work without assuming local config files exist, write the appropriate config
to a temporary file before running commands.

### Mainnet Configuration

```yaml
system_object: 0x2134d52768ea07e8c43570ef975eb3e4c27a39fa6396bef985b5abc58d03ddd2
staking_object: 0x10b9d30c28448939ce6c4d6c6e0ffce4a7f8a4ada8248bdad09ef8b70e4a3904
subsidies_object: 0xb606eb177899edc2130c93bf65985af7ec959a2755dc126c953755e59324209e
```

### Testnet Configuration

```yaml
system_object: 0x6c2547cbbc38025cf3adac45f63cb0a8d12ecf777cdc75a4971612bf97fdf6af
staking_object: 0xbe46180321c30aab2f8b3501e24048377287fa708018a5b7c2792b35fe339ee3
subsidies_object: 0xda799d85db0429765c8291c594d334349ef5bc09220e79ad397b30106161a0af
exchange_objects:
  - 0xf4d164ea2def5fe07dc573992a029e010dba09b1a8dcbc44c5c2e79567f39073
  - 0x19825121c52080bb1073662231cfea5c0e4d905fd13e95f21e9a018f2ef41862
  - 0x83b454e524c71f30803f4d6c302a86fb6a39e96cdfb873c2d1e93bc1c26a3bc5
  - 0x8d63209cf8589ce7aef8f262437163c67577ed09f3e636a9d8e0813843fb8bf1
```

## Committee Information

The next committee is determined in the middle of the epoch. The next committee can be known after
half of the epoch duration has passed. If the `info all` output does not include a "Next committee"
section, it means that the next epoch committee has not been determined yet.

## Workflow

### Step 1: Set Up Configuration

1. Determine the network from the user's argument (mainnet or testnet).
2. Write the corresponding YAML config to a temporary file:
   ```bash
   WALRUS_TMP_CONFIG=$(mktemp /tmp/walrus_XXXXXX_config.yaml)
   ```
3. Write the appropriate config content (from the section above) to this file.

### Step 2: Find the Walrus Binary

Look for the `walrus` binary in this order:
1. `walrus` (in PATH)
2. `~/.local/bin/walrus`
3. `~/.walrus/bin/walrus`

If not found, install it by running the installation command from the Prerequisites section (always
use `-n testnet`). After installation, verify the binary is available before proceeding.

### Step 3: Get Network Overview

Run the following command to get full network information. **IMPORTANT**: Do not truncate the
output — capture the complete response including all node details. The output can be large (60KB+
for 100 nodes) but all of it is needed for accurate analysis.

```bash
walrus --config "$WALRUS_TMP_CONFIG" info all 2>&1
```

This returns:
- **Epoch information**: current epoch, start/end times, duration
- **Storage node summary**: total nodes, total shards
- **Blob size limits**: maximum blob size, storage unit size
- **Storage prices**: per-epoch pricing in FROST and WAL
- **BFT parameters**: fault tolerance (f), quorum threshold (2f+1)
- **Encoding parameters**: shard count, source symbols, metadata/sliver sizes
- **Storage node table**: index, name, shard count, stake, address for every node
- **Per-node details**: node ID, public key, owned shards, price votes, capacity votes

Parse and present this information clearly to the user. Summarize the key metrics at the top level.

**After presenting the network overview, notify the user that they can now ask about individual
node health information** (e.g., "You can now ask about individual node health, specific node
details, stake distribution, shard assignments, or any other network queries."). Then wait for the
user to ask follow-up questions. Do not automatically run health checks or other commands.

### Step 4: Answer User Queries

After presenting the network overview, wait for and answer user queries. Common query types:

#### Global Network Status Queries
- Total nodes, total shards, current epoch
- Stake distribution analysis (which nodes have most/least stake)
- Shard distribution analysis
- Price vote analysis (storage price, write price)
- Capacity vote analysis

Answer these directly from the `info all` output.

#### Individual Node or Group Queries
For queries about specific nodes (by name, index, or node ID), use the health command:

```bash
walrus --config "$WALRUS_TMP_CONFIG" health --node-ids <NODE_ID> --detail
```

For multiple nodes:
```bash
walrus --config "$WALRUS_TMP_CONFIG" health --node-ids <NODE_ID_1> <NODE_ID_2> ... --detail
```

For the entire committee:
```bash
walrus --config "$WALRUS_TMP_CONFIG" health --committee --detail
```

The health command with `--detail` returns per-node:
- **Uptime**: how long the node has been running
- **Current epoch**: the epoch the node is on
- **Node status**: Active, Inactive, etc.
- **Event progress**: events persisted, events pending, highest finished event index
- **Checkpoint downloading progress**: latest checkpoint sequence number
- **Shard summary**: owned shards, read-only shards
- **Owned shard status**: counts of Ready, In transfer, In recovery, Unknown
- **Owned shard details**: per-shard status

Other useful health command options:
- `--sort-by <status|id|name|url>`: sort the output
- `--desc`: sort in descending order
- `--json`: output as JSON for programmatic analysis
- `--node-urls <URL>...`: query by node URL instead of ID
- `--active-set`: query all nodes in the active set

#### Mapping Node Names to Node IDs

The `info all` output includes both node names and node IDs. When the user refers to a node by
name (e.g., "Mysten Labs 0"), look up the corresponding node ID from the `info all` output and use
it with the health command.

### Step 5: Clean Up

After all queries are answered, clean up the temporary config file:
```bash
rm -f "$WALRUS_TMP_CONFIG"
```

## Output Guidelines

- Present network overview in a clear, structured format
- Highlight any anomalies (nodes with shards in recovery, unusually high pending events, etc.)
- When comparing nodes, use tables for readability
- Convert FROST to WAL where helpful (1 WAL = 1,000,000,000 FROST)
- For stake percentages, calculate relative to total network stake
