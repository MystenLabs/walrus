> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

The indexer listens to Sui events emitted by the Walrus Memory contract and uses them to update local backend state.

## Events

The Walrus Memory contract emits the following events:

| Event | Emitted when | Fields |
|-------|-------------|--------|
| `AccountCreated` | A new account is created | `account_id`, `owner` |
| `DelegateKeyAdded` | A delegate key is added | `account_id`, `public_key`, `sui_address`, `label` |
| `DelegateKeyRemoved` | A delegate key is removed | `account_id`, `public_key` |
| `AccountDeactivated` | An account is frozen | `account_id`, `owner` |
| `AccountReactivated` | A frozen account is unfrozen | `account_id`, `owner` |

## Current coverage

The indexer currently targets the `AccountCreated` event flow as its primary sync path. Delegate key events and account activation events are part of the broader design and might be indexed in future iterations.