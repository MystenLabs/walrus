# Walrus S3 Gateway

A fully functional S3-compatible gateway that allows applications to interact with Walrus storage using standard S3 APIs with **client-side signing** for enhanced security.

## 🔑 Key Feature: Client-Side Signing

This implementation uses **client-side signing** where clients sign Sui transactions locally with their wallets before submitting storage operations.

### Workflow

1. **S3 PUT Request**: Client sends standard S3 PUT operation
2. **Transaction Template**: Gateway responds with HTTP 202 + unsigned transaction
3. **Local Signing**: Client signs transaction with their Sui wallet
4. **Transaction Submission**: Client submits signed transaction via new endpoint
5. **Execution**: Gateway validates and executes on Walrus network

```
Client ──PUT──> Gateway ──202+Template──> Client ──Sign──> Wallet
   ↑                                                          │
   └──────────── Submit Signed TX ←─────────────────────────┘
```

## Features

### ✅ Current Implementation

- **🔐 Client-Side Signing**
  - Transaction template generation
  - Signed transaction submission endpoints
  - Complete client control over private keys

- **🌐 S3 Compatibility**
  - PUT/GET/DELETE operations
  - AWS Signature Version 4 authentication
  - Standard S3 metadata support

- **🔒 Security**
  - Private keys never leave client devices
  - Local transaction signing only
  - Gateway validates signatures only

## Quick Start

### 1. Configuration

Create `config.toml`:
```toml
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

### 2. Start the Gateway

```bash
cargo run --bin walrus-s3-gateway -- --config config.toml
```

### 3. Test Client-Side Signing

```bash
./test-client-signing.sh
```

## API Endpoints

### Standard S3 Operations
- `PUT /{bucket}/{object}` - Returns HTTP 202 with signing requirements
- `GET /{bucket}/{object}` - Retrieve objects
- `DELETE /{bucket}/{object}` - Delete objects

### Client-Side Signing Endpoints
- `POST /_walrus/generate-transaction` - Generate transaction templates
- `POST /_walrus/submit-transaction` - Submit signed transactions

## Client Integration

### Handle PUT Response
```javascript
const response = await fetch('http://localhost:9200/bucket/object', {
  method: 'PUT',
  headers: { 'Authorization': 'AWS4-HMAC-SHA256 ...' },
  body: fileData
});

if (response.status === 202) {
  const signingData = await response.json();
  // Sign transaction with wallet
  const signedTx = await signWithSuiWallet(signingData.transaction_template);
  // Submit signed transaction
  await submitSignedTransaction(signedTx, signingData.bucket, signingData.key);
}
```

## Architecture

### Configuration (`src/config.rs`)
- Client-side signing settings
- Walrus endpoint configuration
- Metadata storage options

### Credential Management (`src/credentials.rs`)
- User access key mapping
- Transaction signature validation
- Signing workflow management

### Signing Handlers (`src/handlers/signing.rs`)
- Transaction template generation
- Signed transaction validation
- Error handling for signing failures

## Testing

The `test-client-signing.sh` script provides automated testing:
- ✅ PUT operations return HTTP 202 with signing requirements
- ✅ Transaction template generation works
- ✅ Transaction submission endpoint responds correctly

## Security Benefits

- **Private Key Protection**: Keys never leave client devices
- **Transaction Transparency**: Clients see exactly what they're signing
- **User Control**: Full control over blockchain transactions
- **Validation Only**: Gateway only validates, never creates signatures

## Development

### Build
```bash
cargo build --release
```

### Run Tests
```bash
cargo test
./test-client-signing.sh
```

### Check Code
```bash
cargo check
cargo clippy
```

## Documentation

- [CLIENT-SIDE-SIGNING.md](CLIENT-SIDE-SIGNING.md) - Detailed implementation guide
- [IMPLEMENTATION-SUMMARY.md](IMPLEMENTATION-SUMMARY.md) - Technical summary
- [config.example.toml](config.example.toml) - Configuration examples

## Next Steps

1. **Integration**: Test with real Sui wallets
2. **Performance**: Implement transaction template caching  
3. **Features**: Add GET, DELETE, LIST operations
4. **Production**: Add TLS, monitoring, and logging
