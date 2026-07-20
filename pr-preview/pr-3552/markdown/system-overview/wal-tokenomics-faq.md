> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

This page answers common questions about the WAL token and how value flows through Walrus. It focuses on mechanics that are visible to users and integrators: what you pay, where those payments go, how staking and rewards work, and what actually happens when you burn a blob.

For the authoritative economic model, including token supply and long-term incentive design, see the [Walrus whitepaper](/walrus.pdf). Where a rumor or a third-party summary disagrees with the whitepaper or with `walrus info`, treat those two as the source of truth.

## Fees and fee flows

These questions cover what you pay when you use Walrus and where those payments go.

### What is WAL used for?

WAL is the native token of Walrus. You use it to pay for storage and to stake with storage nodes. Storage is priced at a fixed **$0.023/GB/month** and paid in WAL, with the WAL amount adjusting automatically as the WAL price changes. See [Storage Costs](/docs/system-overview/storage-costs) for the full pricing model.

You also pay **SUI**, not WAL, for the Sui transactions that register, certify, and extend blobs. WAL covers storage and write fees; SUI covers onchain gas.

### Where do my storage fees go?

When you buy storage, your WAL is paid into the **storage fund**, which holds WAL for the epochs your blob is stored across. At the end of each epoch, the fund distributes WAL to storage nodes based on their measured performance, which nodes determine through light audits of each other. Stakers receive a share of these fees as rewards. See [Storage Costs](/docs/system-overview/storage-costs#storage-fund).

### What is the write fee?

Registering a blob costs a WAL write fee in addition to the storage payment. This fee keeps deleting blobs and reusing storage resources sustainable for the system, so that write-heavy workloads pay for the work they create.

### What are subsidies?

On networks that configure an onchain storage subsidy, the subsidy can offset part of the WAL storage cost. A subsidy changes how much WAL a store consumes, but it does not change who signs the store or who pays the SUI gas. The Mainnet subsidies package is listed in the [Network Reference](/docs/network-reference#package-ids). See also [Sponsored and walletless uploads](/docs/sponsored-uploads).

## Staking, re-delegation, and rewards

Walrus runs a delegated proof-of-stake system on the WAL token. Committee changes between epochs are managed by the [staking contracts](https://github.com/MystenLabs/walrus/tree/main/contracts/walrus/sources/staking).

### How do staking rewards work?

Anyone can delegate WAL to storage nodes. Shards are assigned to nodes each epoch roughly in proportion to the stake delegated to them, and by staking you earn a share of the storage fees that node collects. For the full walkthrough, see [Staking and Unstaking](/docs/operator-guide/stake).

### How do I move stake between nodes (re-delegation)?

To move stake from one node to another, unstake from the first node and stake the withdrawn WAL with the second. Both actions follow the timing rules below, so plan a re-delegation around epoch boundaries rather than expecting it to take effect immediately.

### When do staking and unstaking take effect?

Committee selection for an epoch happens ahead of time, at the midpoint of the previous epoch, because moving shards between nodes takes time.

- **Staking:** To affect the committee in epoch `e`, stake before the midpoint of epoch `e - 1`. Stake added after that point first becomes active in epoch `e + 1`.
- **Unstaking:** A withdrawal requested before the midpoint of epoch `e - 1` stops earning at the start of epoch `e`. Requested after that point, the stake stays active and keeps accruing rewards through epoch `e`, and the balance is available at the start of epoch `e + 1`.

### Can my stake be slashed?

Storage nodes are subject to penalties for misbehavior or poor performance, which affect the rewards flowing to a node and its stakers. If you delegate stake, choose reliable nodes. For how penalties work on the operator side, see [Slashing](/docs/operator-guide/storage-nodes/slashing).

## Burning and supply

These questions address the most common points of confusion about WAL, including what the word "burn" means in Walrus.

### Does Walrus burn WAL when I store or delete data?

No. Storing a blob pays WAL into the storage fund, which is distributed to storage nodes as rewards. It is not burned. Deleting a blob does not burn WAL either, and it does not refund your storage payment.

The word "burn" in Walrus documentation almost always refers to burning a blob's **Sui object**, which is unrelated to the WAL token. For questions about WAL token supply and any protocol-level supply mechanics, the [Walrus whitepaper](/walrus.pdf) is the authoritative source. Do not rely on social media summaries for supply claims.

### What does burning a blob actually do?

Burning a blob removes the blob's corresponding **Sui object**, which reclaims most of that object's SUI storage cost through a [Sui storage rebate](https://docs.sui.io/concepts/sui-architecture/sui-storage#storage-rebates). Burning the Sui object:

- Does **not** delete the blob data from Walrus.
- Does **not** refund the WAL you paid for storage.
- Forfeits lifecycle control of the blob, so you can no longer extend a permanent blob or extend or delete a deletable blob.

See [Burn blobs](/docs/walrus-client/managing-blobs#burn-blobs) for the CLI workflow, and [Storage Costs](/docs/system-overview/storage-costs#manage-blob-object-lifecycle) for when burning saves money.

### Why is storage priced in USD but paid in WAL?

Pricing storage at a fixed **$0.023/GB/month** gives you predictable, budgetable costs regardless of WAL price movement. Storage nodes track the WAL price from multiple sources and periodically update an onchain price vote, so the WAL amount charged for a given store adjusts to keep the USD-denominated price stable. See [How pricing works](/docs/system-overview/storage-costs#how-pricing-works).

## References

- [Storage Costs](/docs/system-overview/storage-costs)
- [Staking and Unstaking](/docs/operator-guide/stake)
- [Network Reference](/docs/network-reference)
- [Walrus whitepaper](/walrus.pdf)