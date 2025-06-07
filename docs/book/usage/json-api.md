# JSON mode

All Walrus client commands are also available in JSON mode. In this mode, all the command-line flags
of the original CLI command can be specified in JSON format. The JSON mode therefore simplifies
programmatic access to the CLI.

For example, to store a blob, run:

```sh
walrus json \
    '{
        "config": "path/to/client_config.yaml",
        "command": {
            "store": {
                "files": ["README.md", "LICENSE"],
                "epochs": 100
            }
        }
    }'
```

Or, to read a blob knowing the blob ID:

```sh
walrus json \
    '{
        "config": "path/to/client_config.yaml",
        "command": {
            "read": {
                "blobId": "4BKcDC0Ih5RJ8R0tFMz3MZVNZV8b2goT6_JiEEwNHQo"
            }
        }
    }'
```
### blobStatus

Get the status of a blob (by file or blob ID):

```sh
{
  "config": "path/to/client_config.yaml",
  "command": {
    "blobStatus": {
      "file": "README.md"
    }
  }
}
```

### blobId

Compute the blob ID for a file:

```sh
{
  "config": "path/to/client_config.yaml",
  "command": {
    "blobId": {
      "file": "README.md"
    }
  }
}
```

### convertBlobId

Convert a decimal number into a base64 blob ID:

```sh
{
  "command": {
    "convertBlobId": {
      "blobIdDecimal": "80885466015098902458382552429473803233277035186046880821304527730792838764083"
    }
  }
}
```

### delete

Delete a blob by its ID:

```sh
{
  "command": {
    "delete": {
      "blobIds": ["4BKcDC0Ih5RJ8R0tFMz3MZVNZV8b2goT6_JiEEwNHQo"],
      "yes": true
    }
  }
}
```

### listBlobs

List all registered blobs owned by the wallet:

```sh
{
  "command": {
    "listBlobs": {}
  }
}
```

To include all expired blobs as well:

```sh
{
  "command": {
    "listBlobs": {
      "includeExpired": true
    }
  }
}
```

### burnBlobs

Burn one or more blob objects by object ID:

```sh
{
  "command": {
    "burnBlobs": {
      "objectIds": ["0xabc123", "0xdef456"],
      "yes": true
    }
  }
}
```

Burn all blobs:

```sh
{
  "command": {
    "burnBlobs": {
      "all": true,
      "yes": true
    }
  }
}
```

Burn only expired blobs:

```sh
{
  "command": {
    "burnBlobs": {
      "allExpired": true,
      "yes": true
    }
  }
}
```

### extend

Extend a blob (shared veya owned):

```sh
{
  "command": {
    "extend": {
      "blobObjId": "0xabc123",
      "shared": false,
      "epochsExtended": 100
    }
  }
}
```

### share

Make a blob shared:

```sh
{
  "command": {
    "share": {
      "blobObjId": "0xabc123"
    }
  }
}
```

Make a blob shared and fund it at the same time:

```sh
{
  "command": {
    "share": {
      "blobObjId": "0xabc123",
      "amount": 1000000000
    }
  }
}
```

### getBlobAttribute

Get the attributes of a blob:

```sh
{
  "command": {
    "getBlobAttribute": {
      "blobObjId": "0xabc123"
    }
  }
}
```

### setBlobAttribute

Set multiple key-value attributes on a blob:

```sh
{
  "command": {
    "setBlobAttribute": {
      "blobObjId": "0xabc123",
      "attributes": ["key1", "value1", "key2", "value2"]
    }
  }
}
```

### removeBlobAttributeFields

Remove keys from blob attributes:

```sh
{
  "command": {
    "removeBlobAttributeFields": {
      "blobObjId": "0xabc123",
      "keys": ["key1", "key2"]
    }
  }
}
```

### removeBlobAttribute

Remove the attribute object completely:

```sh
{
  "command": {
    "removeBlobAttribute": {
      "blobObjId": "0xabc123"
    }
  }
}
```

### info

Get general information about the storage system:

```sh
{
  "command": {
    "info": {}
  }
}
```

### info (subcommands)

Example of getting information using a subcommand:

```sh
{
  "command": {
    "info": {
      "command": {
        "epoch": {}
      }
    }
  }
}
```

### info all

Get all available information:

```sh
{
  "command": {
    "info": {
      "command": {
        "all": {}
      }
    }
  }
}
```

### health

Check health status of storage nodes by ID:

```sh
{
  "command": {
    "health": {
      "nodeIds": ["0xabc123"]
    }
  }
}
```

For all nodes in the active committee:

```sh
{
  "command": {
    "health": {
      "committee": true
    }
  }
}
```

### stake

Stake WAL to storage nodes:

```sh
{
  "command": {
    "stake": {
      "nodeIds": ["0xabc123"],
      "amounts": [1000000000]
    }
  }
}
```

Apply the same amount to all nodes:

```sh
{
  "command": {
    "stake": {
      "nodeIds": ["0xabc123", "0xdef456"],
      "amounts": [1000000000]
    }
  }
}
```

### getWal

Exchange SUI (MIST) for WAL:

```sh
{
  "command": {
    "getWal": {
      "amount": 1000000000
    }
  }
}
```

Specify an exchange ID:

```sh
{
  "command": {
    "getWal": {
      "exchangeId": "0xabc123",
      "amount": 1000000000
    }
  }
}
```

### generateSuiWallet

Generate a new Sui wallet:

```sh
{
  "command": {
    "generateSuiWallet": {}
  }
}
```

To receive tokens via faucet:

```sh
{
  "command": {
    "generateSuiWallet": {
      "useFaucet": true
    }
  }
}
```

### fundSharedBlob

Fund an existing shared blob:

```sh
{
  "command": {
    "fundSharedBlob": {
      "sharedBlobObjId": "0xabc123",
      "amount": 1000000000
    }
  }
}
```

### nodeAdmin

Perform admin operations for storage nodes.

```sh
{
  "command": {
    "nodeAdmin": {
      "nodeId": "0xabc123",
      "command": {
        "collectCommission": {}
      }
    }
  }
}
```

### voteForUpgrade

Vote for an upgrade:

```sh
{
  "command": {
    "nodeAdmin": {
      "nodeId": "0xabc123",
      "command": {
        "voteForUpgrade": {
          "upgradeManagerObjectId": "0xmanager123",
          "packagePath": "/path/to/package"
        }
      }
    }
  }
}
```

### setGovernanceAuthorized

Authorize a governance entity:

```sh
{
  "command": {
    "nodeAdmin": {
      "nodeId": "0xabc123",
      "command": {
        "setGovernanceAuthorized": {
          "address": "0x1234...abcd"
        }
      }
    }
  }
}
```

### setCommissionAuthorized

Authorize a commission entity:

```sh
{
  "command": {
    "nodeAdmin": {
      "nodeId": "0xabc123",
      "command": {
        "setCommissionAuthorized": {
          "address": "0x1234...abcd"
        }
      }
    }
  }
}
```

### completion

Generate shell completion scripts.

```sh
{
  "command": {
    "completion": {
      "shell": "bash"
    }
  }
}
```

### daemon.publisher

Run a publisher service at the provided network address.

```sh
{
  "command": {
    "daemon": {
      "publisher": {
        "subWalletsDir": "/path/to/wallets",
        "burnAfterStore": true
      }
    }
  }
}
```

### daemon.aggregator

Run an aggregator service that aggregates data from storage nodes.

```sh
{
  "command": {
    "daemon": {
      "aggregator": {
        "rpcUrl": "https://fullnode.testnet.sui.io:443"
      }
    }
  }
}
```

### daemon.daemon

Run a combined publisher + aggregator service (single process).

```sh
{
  "command": {
    "daemon": {
      "daemon": {
        "subWalletsDir": "/path/to/wallets"
      }
    }
  }
}
```

All options, default values, and commands are equal to those of the "standard" CLI mode, except that
they are written in "camelCase" instead of "kebab-case".

The `json` command also accepts input from `stdin`.

The output of a `json` command will itself be JSON-formatted, again to simplify parsing the results
in a programmatic way. For example, the JSON output can be piped to the `jq` command for parsing and
manually extracting relevant fields.
