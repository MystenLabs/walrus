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

## Important: Sandbox and Network Access

The `walrus` CLI requires outbound network access to Sui RPC endpoints and storage nodes. All
`walrus` commands must be run with `dangerouslyDisableSandbox: true` in the Bash tool, otherwise
connections will fail with "client error (Connect)" or "Operation not permitted" errors.

Similarly, all `curl` requests to Sui GraphQL endpoints and `dig` DNS lookups require
`dangerouslyDisableSandbox: true`.

## Configuration

Since this skill must work without assuming local config files exist, write the appropriate config
to a temporary file before running commands.

The Walrus package ID is not listed below as it is stored as a field in the system and staking
objects and can be read from there.

### Mainnet Configuration

Walrus config (for `--config` flag):
```yaml
system_object: 0x2134d52768ea07e8c43570ef975eb3e4c27a39fa6396bef985b5abc58d03ddd2
staking_object: 0x10b9d30c28448939ce6c4d6c6e0ffce4a7f8a4ada8248bdad09ef8b70e4a3904
```

Additional Sui packages and objects (for on-chain analysis):
```
WAL_PKG_ID=0x356a26eb9e012a68958082340d4c4116e7f55615cf27affcff209cf0ae544f59
UPGRADE_MANAGER_OBJECT=0xc42868ad4861f22bd1bcd886ae1858d5c007458f647a49e502d44da8bbd17b51
WALRUS_SUBSIDIES_PKG_ID=0x14b874da49e152d2b2910122330f7eb925d75bdb0a0f8e2c6b9b1162a5560a8c
WALRUS_SUBSIDIES_OBJECT=0xb2ce8bd6e372ea93422a167b52d1ac367d080f67a6c4356334aca8e96ba0577a
```

### Testnet Configuration

Walrus config (for `--config` flag):
```yaml
system_object: 0x6c2547cbbc38025cf3adac45f63cb0a8d12ecf777cdc75a4971612bf97fdf6af
staking_object: 0xbe46180321c30aab2f8b3501e24048377287fa708018a5b7c2792b35fe339ee3
exchange_objects:
  - 0xf4d164ea2def5fe07dc573992a029e010dba09b1a8dcbc44c5c2e79567f39073
  - 0x19825121c52080bb1073662231cfea5c0e4d905fd13e95f21e9a018f2ef41862
  - 0x83b454e524c71f30803f4d6c302a86fb6a39e96cdfb873c2d1e93bc1c26a3bc5
  - 0x8d63209cf8589ce7aef8f262437163c67577ed09f3e636a9d8e0813843fb8bf1
```

Additional Sui packages and objects (for on-chain analysis):
```
WAL_PKG_ID=0x8270feb7375eee355e64fdb69c50abb6b5f9393a722883c1cf45f8e26048810a
WAL_EXCHANGE_PKG_ID=0x82593828ed3fcb8c6a235eac9abd0adbe9c5f9bbffa9b1e7a45cdd884481ef9f
WALRUS_SUBSIDIES_PKG_ID=0x51b0bdfba7ac0ac232402fbb36e17769e41a5c93bb1271f3b7daf1cfea81ca4c
WALRUS_SUBSIDIES_OBJECT=0x21432c30c510a27432bda9349d9f3f0aff5b84285c369f67dfd3d3ef4cf4eb35
```

## Committee Information

The next committee is determined in the middle of the epoch. The next committee can be known after
half of the epoch duration has passed. If the `info all` output does not include a "Next committee"
section, it means that the next epoch committee has not been determined yet.

## Workflow

### Step 1: Set Up Configuration

1. Determine the network from the user's argument (mainnet or testnet).
2. Write the corresponding YAML config to `/tmp/walrus_inspect_config.yaml` using
   `dangerouslyDisableSandbox: true`. This is required because the walrus commands also run with
   sandbox disabled and must be able to read this file.
3. Write the appropriate config content (from the section above) to this file.

### Step 2: Find the Walrus Binary

Look for the `walrus` binary in this order:
1. `walrus` (in PATH)
2. `~/.local/bin/walrus`
3. `~/.walrus/bin/walrus`

If not found, install it by running the installation command from the Prerequisites section (always
use `-n testnet`). After installation, verify the binary is available before proceeding.

### Step 3: Get Network Overview and Committee Health

Run both commands **in parallel** to get full network information and committee health as JSON.
Save outputs to files for jq processing:

```bash
RUST_LOG=off walrus --config /tmp/walrus_inspect_config.yaml info all --json > /tmp/walrus_info.json 2>&1
RUST_LOG=off walrus --config /tmp/walrus_inspect_config.yaml health --committee --detail --json > /tmp/walrus_health.json 2>&1
```

Both commands must use `dangerouslyDisableSandbox: true`. After both complete, use the jq scripts
from the "Parsing" sections below to extract the summary.

**IMPORTANT**: Always use `--json` for structured output. The output can be large (60-200KB for
100 nodes) — always parse with jq, never read raw files or delegate to agents.

**Exception**: When re-checking unreachable nodes (see Step 3b), run the health command **without**
`--json` to get detailed error reasons (see below).

#### Parsing the `info all` output

The jq scripts are committed alongside this skill file. Use them directly:

```bash
jq -f .claude/skills/inspect-walrus-network/info_overview.jq /tmp/walrus_info.json
```

#### Parsing the `health --committee --detail` output

**Step 1**: Run the health analysis jq script:
```bash
jq -f .claude/skills/inspect-walrus-network/health_analysis.jq /tmp/walrus_health.json
```

**Step 2**: For any problematic nodes found above, extract detail:
```bash
jq --argjson names '["NODE_NAME_1", "NODE_NAME_2"]' \
  -f .claude/skills/inspect-walrus-network/health_detail.jq /tmp/walrus_health.json
```

Present the network overview and health summary to the user. Highlight any unhealthy nodes. Then
notify the user that they can ask follow-up questions about individual nodes, stake distribution,
shard assignments, or other network queries.

### Step 3b: Re-check Unreachable Nodes

The bulk committee health check can produce transient failures (timeouts when querying 100 nodes
in parallel). Always re-check nodes that appeared unreachable before reporting them as down.

1. Extract the node IDs of unreachable nodes from the initial health output.
2. Re-run the health command for just those nodes **without `--json`**:
   ```bash
   walrus --config "/tmp/walrus_inspect_config.yaml" health --node-ids <ID1> <ID2> ... --detail 2>&1
   ```
   The non-JSON output includes detailed error information, such as:
   - `hyper_util::client::legacy::Error(Connect, TimedOut)` — connection timed out
   - `ConnectError("dns error", ...)` — DNS resolution failure
   - `ConnectError("tcp connect error", <IP>:<PORT>, Os { code: 61, ..., message: "Connection refused" })` — connection refused
   The `--json` output only provides a generic "error sending request for url" message and loses
   these details.
3. Nodes that now respond successfully were transient failures — report them as healthy.
4. For nodes that are still unreachable, **always record the specific failure reason** (timeout,
   DNS failure, connection refused) in the report.

### Step 4: Answer User Queries

After presenting the overview, wait for and answer user queries. Common query types:

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
walrus --config "/tmp/walrus_inspect_config.yaml" health --node-ids <NODE_ID> --detail --json
```

For multiple nodes:
```bash
walrus --config "/tmp/walrus_inspect_config.yaml" health --node-ids <NODE_ID_1> <NODE_ID_2> ... --detail --json
```

**IMPORTANT**: Always use `--json` for structured output. This makes it much easier to
programmatically analyze node health, compare event numbers, and detect anomalies.

The health command with `--detail` returns per-node:
- **Uptime**: how long the node has been running
- **Current epoch**: the epoch the node is on
- **Node status**: Active, RecoveryInProgress, etc.
- **Event progress**: events persisted, events pending, highest finished event index
- **Checkpoint downloading progress**: latest checkpoint sequence number, estimated lag
- **Shard summary**: owned shards, read-only shards
- **Owned shard status**: counts of Ready, In transfer, In recovery, Unknown
- **Owned shard details**: per-shard status

Other useful health command options:
- `--sort-by <status|id|name|url>`: sort the output
- `--desc`: sort in descending order
- `--node-urls <URL>...`: query by node URL instead of ID
- `--active-set`: query all nodes in the active set

#### Mapping Node Names to Node IDs

The `info all` output includes both node names and node IDs. When the user refers to a node by
name (e.g., "Mysten Labs 0"), look up the corresponding node ID from the `info all` output and use
it with the health command.

#### Health Analysis: What to Look For

When analyzing the health output, check for these categories of issues:

**Unreachable nodes**: Nodes where the health endpoint returns an error. Common failure modes:
- Connection timed out — node may be down or firewalled
- Connection refused — node process not running on the expected port
- DNS resolution failure — hostname cannot be resolved

**Event processing issues**: Compare `events_persisted` and `highest_finished_event_index` across
all nodes. Healthy nodes should have very similar values (within single digits of each other).
Watch for:
- Nodes with `highest_finished_event_index: 0` — likely stuck and not processing events
- Nodes significantly behind the max `events_persisted` (>100K behind) — lagging
- Nodes with large `events_pending` (>10K) — catching up but behind
- Nodes with identical suspicious values — may be running from the same stale snapshot

**Epoch mismatch**: All nodes should be on the current epoch. A node on a previous epoch has
failed to transition and needs operator intervention. A node with epoch mismatch **and** a
drastically different event count from the rest of the fleet likely has its registered public host
pointing at a node on a different network (e.g., testnet entry pointing at a mainnet node).

**IMPORTANT**: Always check for epoch-mismatch nodes **before** interpreting event laggard data.
A single node connected to a different network can have a much higher or lower event count,
skewing the max/min and making the entire fleet appear as laggards. Exclude epoch-mismatch nodes
when computing event statistics.

**Checkpoint lag**: Compare `estimated_checkpoint_lag` across nodes. Nodes with significantly
higher lag (e.g., 146M vs 53M) are not keeping up with the chain.

**Shard issues**: Check for shards in recovery or transfer. A few shards in transfer during epoch
transitions is normal, but shards stuck in recovery may indicate storage problems.

**Low uptime**: Nodes with very low uptime (minutes instead of days) may be crash-looping. Flag
these for operator investigation.

### Step 5: DNS Troubleshooting

When nodes fail with DNS resolution errors, read and follow the instructions in
`.claude/skills/inspect-walrus-network/dns-troubleshooting.md`.

### Step 6: Composing Operator Notifications

When the user asks to compose a notification message, read and follow the instructions in
`.claude/skills/inspect-walrus-network/operator-notifications.md`.

### Step 7: On-Chain Analysis and Subsidies

For on-chain queries (subsidies, transactions, object history), read and follow the instructions in
`.claude/skills/inspect-walrus-network/on-chain-analysis.md`.

### Step 8: Clean Up

After all queries are answered, clean up temporary files:
```bash
rm -f /tmp/walrus_inspect_config.yaml /tmp/walrus_info.json /tmp/walrus_health.json
```

## Output Guidelines

- Present network overview in a clear, structured format
- Highlight any anomalies (nodes with shards in recovery, unusually high pending events, etc.)
- When comparing nodes, use tables for readability
- Convert FROST to WAL where helpful (1 WAL = 1,000,000,000 FROST)
- For stake percentages, calculate relative to total network stake
