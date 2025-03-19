# Available Networks

## Mainnet configuration

## Testnet configuration

### Testnet WAL faucet

The Walrus Testnet uses Testnet WAL tokens to buy storage and stake. Testnet WAL tokens have no
value and can be exchanged (at a 1:1 rate) for some Testnet SUI tokens, which also have no value,
through the following command:

```sh
walrus get-wal
```

You can check that you have received Testnet WAL by checking the Sui balances:

```sh
sui client balance
╭─────────────────────────────────────────╮
│ Balance of coins owned by this address  │
├─────────────────────────────────────────┤
│ ╭─────────────────────────────────────╮ │
│ │ coin  balance (raw)     balance     │ │
│ ├─────────────────────────────────────┤ │
│ │ Sui   8869252670        8.86 SUI    │ │
│ │ WAL   500000000         0.50 WAL    │ │
│ ╰─────────────────────────────────────╯ │
╰─────────────────────────────────────────╯
```

By default, 0.5 SUI are exchanged for 0.5 WAL, but a different amount of SUI may be exchanged using
the `--amount` option (the value is in MIST/FROST), and a specific SUI/WAL exchange object may be
used through the `--exchange-id` option. The `walrus get-wal --help` command provides more
information about those.


## Running a local Walrus network