# Walrus S3 Gateway - Client-Side Signing Implementation

This document describes the client-side signing feature implementation for the Walrus S3 Gateway, which enables users to sign transactions locally using their Sui wallets before submitting blob storage operations.

## Overview

The client-side signing feature transforms the traditional S3 gateway approach by requiring clients to:

1. **Generate transaction templates** via the gateway API
2. **Sign transactions locally** using their Sui wallet
3. **Submit signed transactions** back to the gateway for execution

This approach provides enhanced security and user control over blockchain transactions while maintaining S3 API compatibility.

## Architecture

```
┌─────────────┐    ┌─────────────────┐    ┌─────────────┐    ┌─────────────┐
│   S3 Client │    │ Walrus S3       │    │ Sui Wallet  │    │   Walrus    │
│             │───▶│   Gateway       │───▶│             │───▶│  Network    │
│             │    │ (Client-Side    │    │ (Local)     │    │             │
└─────────────┘    │  Signing Mode)  │    └─────────────┘    └─────────────┘
                   └─────────────────┘
```

### Key Components

- **Config System**: Enhanced to support client-side signing configuration
- **Credential Manager**: Manages user credentials and transaction signing requirements
- **Signing Handlers**: New endpoints for transaction template generation and submission
- **S3 Compatibility**: Modified S3 handlers to integrate with client-side signing workflow

## Configuration

### Basic Configuration

```toml
# config.toml
listen_address = "127.0.0.1:9200"

[client_signing]
require_signatures = true
sui_rpc_url = "https://fullnode.testnet.sui.io:443"

[walrus]
publisher_url = "https://publisher.walrus-testnet.walrus.space"
aggregator_url = "https://aggregator.walrus-testnet.walrus.space"

[metadata]
storage_type = "file"
storage_path = "./s3_metadata"
```

### Configuration Options

| Option | Description | Default |
|--------|-------------|---------|
| `client_signing.require_signatures` | Enable client-side signing mode | `false` |
| `client_signing.sui_rpc_url` | Sui RPC endpoint for transaction building | Required |
| `walrus.publisher_url` | Walrus publisher endpoint | Required |
| `walrus.aggregator_url` | Walrus aggregator endpoint | Required |

## API Endpoints

### Standard S3 Operations

When client-side signing is enabled, standard S3 operations return special responses:

#### PUT Object
```bash
curl -X PUT http://localhost:9200/bucket/object \
  -H "Authorization: AWS4-HMAC-SHA256 ..." \
  --data-binary @file.txt
```

**Response (HTTP 202):**
```json
{
  "action": "client_signing_required",
  "transaction_template": {
    "transaction_data": "...",
    "gas_budget": 1000000,
    "gas_price": 1000
  },
  "instructions": "Sign this transaction with your Sui wallet and submit via POST to /_walrus/submit-transaction",
  "bucket": "bucket",
  "key": "object"
}
```

### Client-Side Signing Endpoints

#### Generate Transaction Template
```bash
POST /_walrus/generate-transaction
Content-Type: application/json
Authorization: AWS4-HMAC-SHA256 ...

{
  "access_key": "user_access_key",
  "purpose": {
    "StoreBlob": {
      "size": 1024
    }
  }
}
```

**Response:**
```json
{
  "transaction_template": {
    "transaction_data": "base64_encoded_transaction",
    "gas_budget": 1000000,
    "gas_price": 1000
  },
  "signing_instructions": "Sign with your Sui wallet"
}
```

#### Submit Signed Transaction
```bash
POST /_walrus/submit-transaction
Content-Type: application/json

{
  "signed_transaction": "base64_encoded_signed_transaction",
  "bucket": "bucket",
  "key": "object",
  "blob_data": "base64_encoded_blob_data"
}
```

**Response:**
```json
{
  "status": "success",
  "blob_id": "0x...",
  "transaction_digest": "0x...",
  "etag": "\"blob_id\""
}
```

## Client Implementation Guide

### Step 1: Regular S3 Operation
```javascript
// Standard S3 PUT operation
const response = await fetch('http://localhost:9200/bucket/object', {
  method: 'PUT',
  headers: {
    'Authorization': 'AWS4-HMAC-SHA256 ...',
    'Content-Type': 'text/plain'
  },
  body: fileData
});
```

### Step 2: Handle Client Signing Requirement
```javascript
if (response.status === 202) {
  const signingData = await response.json();
  
  if (signingData.action === 'client_signing_required') {
    // Extract transaction template
    const template = signingData.transaction_template;
    
    // Sign with Sui wallet (implementation depends on wallet)
    const signedTx = await signWithSuiWallet(template.transaction_data);
    
    // Submit signed transaction
    const submitResponse = await fetch('http://localhost:9200/_walrus/submit-transaction', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        signed_transaction: signedTx,
        bucket: signingData.bucket,
        key: signingData.key,
        blob_data: btoa(fileData) // base64 encode
      })
    });
  }
}
```

### Step 3: Sui Wallet Integration
```javascript
// Example with Sui Wallet Adapter
import { WalletContextProvider, useWallet } from '@mysten/wallet-adapter-react';

function useWalrusUpload() {
  const { signTransaction } = useWallet();
  
  const uploadToWalrus = async (bucket, key, data) => {
    // 1. Attempt S3 PUT
    const response = await putObject(bucket, key, data);
    
    // 2. Handle signing requirement
    if (response.status === 202) {
      const { transaction_template, bucket, key } = await response.json();
      
      // 3. Sign transaction
      const signedTx = await signTransaction({
        transaction: transaction_template.transaction_data
      });
      
      // 4. Submit signed transaction
      return await submitSignedTransaction({
        signed_transaction: signedTx,
        bucket,
        key,
        blob_data: btoa(data)
      });
    }
    
    return response;
  };
  
  return { uploadToWalrus };
}
```

## Security Considerations

### Private Key Management
- **Never send private keys** to the gateway
- **Always sign locally** using trusted wallet software
- **Verify transaction details** before signing

### Transaction Validation
- **Review gas costs** before signing
- **Validate blob size** matches expectations
- **Check destination addresses** in transaction

### Network Security
- **Use HTTPS** in production environments
- **Validate SSL certificates** for all endpoints
- **Implement request signing** for API authentication

## Testing

### Run Basic Tests
```bash
# Start the gateway
cargo run --bin walrus-s3-gateway -- --config test-config.toml

# Run test script
./test-client-signing.sh
```

### Integration Tests
```bash
# Run full integration test suite
cargo test --package walrus-s3-gateway --test integration_tests
```

## Troubleshooting

### Common Issues

#### "Client signing required" responses
- **Cause**: Client-side signing is enabled in configuration
- **Solution**: Implement proper signing workflow in your client

#### Transaction signing failures
- **Cause**: Invalid transaction format or wallet connection issues
- **Solution**: Verify wallet connection and transaction structure

#### Gas estimation errors
- **Cause**: Insufficient gas budget or network issues
- **Solution**: Increase gas budget or check Sui network status

### Debug Mode
```toml
[logging]
level = "debug"
```

### Logs Location
- Console output with structured logging
- Configure log levels per module if needed

## Migration Guide

### From Server-Side to Client-Side Signing

1. **Update Configuration**:
   ```toml
   [client_signing]
   require_signatures = true
   sui_rpc_url = "https://fullnode.testnet.sui.io:443"
   ```

2. **Update Client Code**:
   - Handle HTTP 202 responses
   - Implement transaction signing
   - Add signed transaction submission

3. **Test Implementation**:
   ```bash
   # Run the test script
   ./test-client-signing.sh
   ```

## Testing

### Quick Test
```bash
# Start the gateway
cargo run --bin walrus-s3-gateway -- --config test-config.toml

# Run test script
./test-client-signing.sh
```

### Expected Results
- PUT operations return HTTP 202 with client signing requirements
- Transaction template generation works
- Transaction submission endpoint responds correctly
