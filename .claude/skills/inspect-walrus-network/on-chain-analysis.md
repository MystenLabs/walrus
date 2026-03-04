# On-Chain Analysis via Sui GraphQL

For on-chain queries (subsidies, transactions, object history), use the Sui GraphQL API instead of
JSON-RPC. All `curl` commands require `dangerouslyDisableSandbox: true`.

## GraphQL Endpoint

```
https://graphql.mainnet.sui.io/graphql
```

## Schema Quick Reference

The Sui GraphQL schema differs from the older JSON-RPC naming. Key types:

- **Root query**: `transactions` (not `transactionBlocks`), `objects`, `checkpoint`, `events`
- **Transaction**: fields are `digest`, `effects`, `kind`, `sender`, `gasInput`, `signatures`
  - No `timestamp` directly -- use `effects { timestamp }` or `effects { checkpoint { timestamp } }`
- **TransactionKind**: union type with `ProgrammableTransaction`, `GenesisTransaction`, etc.
- **ProgrammableTransaction**: has `inputs` and `commands` (a `CommandConnection`)
- **MoveCallCommand**: has `function` (a `MoveFunction` with `name`, `module { name, package { address } }`)
- **TransactionFilter**: supports `affectedObject`, `function`, `kind`, `sentAddress`, etc.

## Example: Query Transactions Affecting an Object

```graphql
{
  transactions(last: 10, filter: { affectedObject: "<OBJECT_ID>" }) {
    nodes {
      digest
      effects { timestamp }
      kind {
        ... on ProgrammableTransaction {
          commands {
            nodes {
              __typename
              ... on MoveCallCommand {
                function { name module { name package { address } } }
              }
            }
          }
        }
      }
    }
  }
}
```

## Querying Object Version History

Use `objectVersionsBefore` (or `objectVersionsAfter`) on any `Object`:

```graphql
{
  object(address: "<ADDR>") {
    objectVersionsBefore(last: 10) {
      nodes {
        version
        asMoveObject { contents { json } }
        previousTransaction { digest effects { timestamp } }
      }
    }
  }
}
```

## Dynamic Fields and Historical Versions

Dynamic field values at historical versions **cannot** be queried via the parent object. Instead:

1. Get the dynamic field wrapper object's address from the current state:
   ```graphql
   {
     object(address: "<PARENT_ADDR>") {
       dynamicFields(first: 1) {
         nodes { address version }
       }
     }
   }
   ```
2. Query the wrapper object's version history directly using its address:
   ```graphql
   {
     object(address: "<WRAPPER_ADDR>") {
       objectVersionsBefore(last: 15) {
         nodes {
           version
           asMoveObject { contents { json } }
           previousTransaction { digest effects { timestamp } }
         }
       }
     }
   }
   ```

## Subsidies Analysis

When asked about subsidy payouts, query the Walrus subsidies object on-chain. See the main skill
file's Configuration section for subsidies object addresses.

### Subsidies Object Structure

The `WalrusSubsidies` object has a dynamic field containing `WalrusSubsidiesInnerV1` with key
fields:
- `subsidy_pool`: remaining WAL funds (in FROST)
- `base_subsidy`: base subsidy per epoch (in FROST)
- `subsidy_per_shard`: subsidy per shard per epoch (in FROST)
- `latest_epoch`: last epoch for which subsidies were processed
- `already_subsidized_balances`: ring buffer of pre-paid storage balances per future epoch

### Finding Subsidy Payout Transactions

Query transactions that affected the subsidies object:
```graphql
{
  transactions(last: 10, filter: {
    affectedObject: "<WALRUS_SUBSIDIES_OBJECT>"
  }) {
    nodes {
      digest
      effects { timestamp }
      kind {
        ... on ProgrammableTransaction {
          commands { nodes { __typename ... on MoveCallCommand {
            function { name module { name } }
          } } }
        }
      }
    }
  }
}
```

Look for calls to `walrus_subsidies::process_subsidies`.

### Computing Payout Amounts

The payout amount is the decrease in `subsidy_pool` between consecutive versions of the
subsidies object. To compute:

1. Find the dynamic field wrapper object address (see "Dynamic Fields and Historical Versions").
2. Query its version history via `objectVersionsBefore`.
3. For each pair of consecutive versions, compute `previous_pool - current_pool`.
4. The inner object JSON contains the `subsidy_pool` field in the `value` key.

### USD Conversion

When requested, fetch the current WAL/USD spot price:
```bash
curl -s https://api.coinbase.com/v2/prices/WAL-USD/spot
```
Returns JSON with `data.amount` containing the USD price. Note that historical payouts are
converted at today's spot price, not the price at payout time.
