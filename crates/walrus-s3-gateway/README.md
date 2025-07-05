# Walrus S3 Gateway

Una gateway S3-compatibile completamente funzionale che permette alle applicazioni di interagire con lo storage Walrus utilizzando API S3 standard. Questo abilita le applicazioni esistenti basate su S3 a utilizzare seamlessly Walrus come backend di storage senza modifiche al codice.

## 🔑 **Caratteristica Principale: Client-Side Signing**

Questa implementazione introduce il **client-side signing**, una funzionalità di sicurezza avanzata che mantiene il controllo delle chiavi private sul lato client mentre fornisce piena compatibilità S3.

### ✅ **Architettura Client-Side Signing**

```
┌─────────────┐    ┌─────────────────┐    ┌─────────────┐    ┌─────────────┐
│   S3 Client │───▶│ Walrus S3       │───▶│ Sui Wallet  │───▶│   Walrus    │
│             │    │   Gateway       │    │ (Local)     │    │  Network    │
│             │    │ (Client-Side    │    │             │    │             │
└─────────────┘    │  Signing Mode)  │    └─────────────┘    └─────────────┘
                   └─────────────────┘
```

### 🔄 **Workflow**

1. **S3 PUT Request**: Il client invia un'operazione S3 PUT standard
2. **Transaction Template**: Il gateway risponde con HTTP 202 + transazione non firmata
3. **Local Signing**: Il client firma la transazione con il proprio wallet Sui
4. **Transaction Submission**: Il client sottomette la transazione firmata tramite nuovo endpoint
5. **Execution**: Il gateway esegue la transazione firmata sulla rete Walrus

## Features

### ✅ **Implementazione Corrente**

- **🔐 Client-Side Signing**
  - Generazione di template di transazione non firmati
  - Endpoint per la sottomissione di transazioni firmate
  - Controllo completo delle chiavi private lato client
  - Trasparenza totale delle transazioni blockchain

- **🌐 Compatibilità S3 Completa**
  - List buckets (`GET /`)
  - Operazioni bucket (GET, PUT, DELETE, HEAD)
  - Operazioni oggetti (GET, DELETE, HEAD, PUT)
  - List objects con paginazione e filtri
  - Supporto metadata e copying oggetti

- **🔒 Autenticazione & Sicurezza**
  - AWS Signature Version 4 (SigV4) authentication
  - Chiavi di accesso configurabili
  - CORS support per applicazioni web
  - TLS/HTTPS support

- **⚙️ Integrazione Walrus**
  - Connessione diretta alla rete Walrus
  - Configurazione automatica client Sui
  - Gestione committee refresh e connessioni

- **🚀 Funzionalità Production-Ready**
  - Logging strutturato con livelli configurabili
  - Gestione graceful shutdown
  - Error handling completo con codici S3-compatibili
  - Supporto file di configurazione (formato TOML)
  - Parsing e validazione argomenti CLI
- Object versioning and lifecycle management
- Bucket policies and access control
- Metrics and monitoring integration
- Performance optimizations and caching

## Quick Start

### 1. Build and Run

```bash
# Build the gateway
cargo build --bin walrus-s3-gateway

# Run with default settings (read-only mode)
cargo run --bin walrus-s3-gateway

# Or with custom configuration
cargo run --bin walrus-s3-gateway -- --bind 0.0.0.0:8080 --access-key mykey --secret-key mysecret
## Quick Start

### 1. Configurazione

Crea un file di configurazione `config.toml`:

```toml
listen_address = "127.0.0.1:9200"

[client_signing]
require_signatures = true
sui_rpc_url = "https://fullnode.testnet.sui.io:443"

[walrus]
publisher_url = "https://publisher.walrus-testnet.walrus.space"
aggregator_url = "https://aggregator.walrus-testnet.walrus.space"

[aws]
access_key_id = "your-access-key"
secret_access_key = "your-secret-key"
region = "us-east-1"

[metadata]
storage_type = "file"
storage_path = "./s3_metadata"

[logging]
level = "info"
```

### 2. Avviare il Gateway

```bash
# Build del progetto
cargo build --release --bin walrus-s3-gateway

# Avvio con configurazione
./target/release/walrus-s3-gateway --config config.toml
```

### 3. Test del Client-Side Signing

```bash
# Esegui il test script
./test-client-signing.sh
```

## API Endpoints

### Standard S3 Operations

Le operazioni S3 standard ora richiedono client-side signing:

```bash
# PUT Object - restituisce template di transazione (HTTP 202)
curl -X PUT http://localhost:9200/bucket/object \
  -H "Authorization: AWS4-HMAC-SHA256 ..." \
  -H "Content-Type: text/plain" \
  --data-binary @file.txt
```

**Risposta (HTTP 202):**
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
committee_refresh_interval = 300
request_timeout = 30
enable_metrics = false
```

## Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   S3 Client     │    │ Walrus S3       │    │ Walrus Network  │
│   (AWS CLI,     │────▶│ Gateway         │────▶│                 │
│    SDK, etc.)   │    │                 │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                              │
                              ▼
                       ┌─────────────────┐
                       │ Sui Testnet     │
                       │ (Metadata &     │
                       │  Transactions)  │
                       └─────────────────┘
```

### Components

1. **S3 API Layer**: Complete implementation of S3 REST API endpoints
2. **Authentication**: SigV4 authentication compatible with AWS standards
3. **Walrus Client**: Read-only client for accessing Walrus testnet
4. **Sui Integration**: Connection to Sui testnet for metadata and contracts

## Current Status

### ✅ **What Works Now**
- Server starts and connects to Walrus testnet
- All S3 API endpoints respond correctly
- Authentication layer processes requests and extracts credentials
- **Write operations (PUT/POST) fully implemented**
- **Per-request client creation with user credentials**
- **Automatic credential mapping from S3 to Walrus**
- Error handling with proper S3 error codes
- Configuration and logging systems

### 🔄 **Read-Write Mode**
The gateway now operates in full read-write mode:
- **READ operations**: Use shared read-only client for efficiency
- **WRITE operations**: Create per-request client with user credentials
- **Credential mapping**: S3 access keys map to Walrus wallet configurations
- **Namespace isolation**: Buckets are implemented as prefixes for object keys

### 🎯 **Key Features**
1. **Per-Request Authentication**
   - Each write operation extracts credentials from S3 authorization header
   - Creates a new Walrus client with user's wallet configuration
   - Ensures complete isolation between users

2. **Credential Mapping Strategy**
   ```rust
   // S3 access key -> Walrus wallet keystore path
   keystore_path = "~/.sui/sui_config/sui.keystore.{access_key}"
   
   // Wallet configuration per user
   active_env = "testnet"
   active_address = determined_from_keystore
   ```

3. **Bucket Namespace Implementation**
   - Walrus doesn't have buckets, so we use prefixes
   - Object key format: `{bucket_name}/{object_key}`
   - Allows S3 bucket semantics on Walrus storage

## Testing

### Manual Testing
```bash
# Start the server
RUST_LOG=info cargo run --bin walrus-s3-gateway

# In another terminal, test various endpoints
curl -v http://localhost:8080/                              # List buckets
curl -v http://localhost:8080/bucket                        # Bucket operations
curl -v -X PUT http://localhost:8080/bucket                 # Create bucket (read-only)
curl -v http://localhost:8080/bucket/object.txt             # Get object
```

### Integration with Applications

The gateway is designed to be a drop-in replacement for S3. Any application that works with S3 should work with the Walrus S3 Gateway by simply changing the endpoint URL.

**Example with popular libraries:**

```python
# Python boto3
import boto3
s3 = boto3.client('s3', 
    endpoint_url='http://localhost:8080',
    aws_access_key_id='walrus-access-key',
    aws_secret_access_key='walrus-secret-key'
)

# Node.js AWS SDK
const AWS = require('aws-sdk');
const s3 = new AWS.S3({
    endpoint: 'http://localhost:8080',
    accessKeyId: 'walrus-access-key',
    secretAccessKey: 'walrus-secret-key'
});
```

## Development

### Project Structure
```
src/
├── bin/walrus-s3-gateway.rs    # Main binary entry point
├── lib.rs                      # Library exports  
├── auth.rs                     # SigV4 authentication
├── config.rs                   # Configuration management
├── error.rs                    # Error types and S3 error codes
├── server.rs                   # HTTP server and Walrus client setup
├── utils.rs                    # Utility functions
├── metadata.rs                 # Object metadata handling
└── handlers/                   # S3 API endpoint handlers
    ├── mod.rs                  # Handler module exports
    ├── bucket.rs               # Bucket operations
    └── object.rs               # Object operations
```

### Adding New Features
1. **New S3 Operations**: Add handlers in `src/handlers/`
2. **Authentication Methods**: Extend `src/auth.rs`
3. **Storage Backends**: Modify `src/server.rs` client creation
4. **Configuration Options**: Update `src/config.rs`

## User Credential Setup

### Setting Up User Wallets

Since the gateway creates per-request clients with user credentials, each user needs their own Sui wallet configured:

1. **Install Sui CLI**
   ```bash
   # Install Sui CLI
   cargo install --locked --git https://github.com/MystenLabs/sui.git --branch testnet sui
   ```

2. **Create User Wallet**
   ```bash
   # Create a new wallet for the user
   sui client new-address ed25519
   
   # Get testnet SUI tokens
   sui client faucet --address YOUR_ADDRESS
   
   # Set environment to testnet
   sui client switch --env testnet
   ```

3. **Configure Access Key Mapping**
   
   The gateway maps S3 access keys to Sui wallet keystores using this pattern:
   ```
   S3 Access Key: "user-123"
   Wallet Keystore: "~/.sui/sui_config/sui.keystore.user-123"
   ```

4. **Copy Keystore for User**
   ```bash
   # Copy the user's keystore to the mapped location
   cp ~/.sui/sui_config/sui.keystore ~/.sui/sui_config/sui.keystore.user-123
   ```

### Example User Setup

```bash
# User "alice" wants to use the gateway
export USER_ACCESS_KEY="alice"
export USER_SECRET_KEY="alice-secret-123"

# Create Sui wallet for Alice
sui client new-address ed25519

# Get some testnet SUI
sui client faucet

# Copy keystore to the expected location
cp ~/.sui/sui_config/sui.keystore ~/.sui/sui_config/sui.keystore.alice

# Now Alice can use AWS CLI with these credentials
aws configure set aws_access_key_id alice
aws configure set aws_secret_access_key alice-secret-123
aws configure set region us-east-1

# Upload a file
aws --endpoint-url http://localhost:8080 s3 cp file.txt s3://mybucket/file.txt
```

### ⚠️ **Critical Security Issue - Current Architecture**

**The current implementation has a significant security flaw**: the server must store all user wallets (private keys) to perform write operations. This creates:

1. **🔥 Security Risk**: Single point of failure - server compromise exposes all user wallets
2. **📈 Scalability Problem**: Each new user requires manual wallet setup on the server
3. **🤝 Trust Issue**: Users must give their private keys to the server operator
4. **🔐 Key Management Nightmare**: Backup, rotation, and recovery for hundreds/thousands of users

### 🛡️ **Secure Architecture Alternatives**

The `credentials.rs` module implements several secure alternatives:

#### **Option 1: Client-Side Transaction Signing**

Instead of the server holding user wallets, clients sign Sui transactions locally:

```mermaid
sequenceDiagram
    participant Client
    participant Gateway
    participant Walrus
    participant Sui

    Client->>Gateway: PUT /bucket/object (with S3 auth)
    Gateway->>Gateway: Validate S3 signature
    Gateway->>Walrus: Prepare blob upload
    Walrus->>Gateway: Return unsigned transaction
    Gateway->>Client: Return unsigned transaction
    Client->>Client: Sign transaction with local wallet
    Client->>Gateway: Submit signed transaction
    Gateway->>Sui: Submit signed transaction
    Sui->>Gateway: Transaction result
    Gateway->>Client: S3 PUT response
```

**Implementation:**
```rust
// Extended S3 API for transaction signing
PUT /bucket/object?prepare=true  // Returns unsigned transaction
PUT /bucket/object?submit=true   // Submits signed transaction
```

#### **Option 2: JWT/Token-Based Authentication**

Use external authentication systems:

```rust
// User authenticates with external system
// Receives JWT with Sui address claims
// Gateway validates JWT and uses address for operations

#[derive(Deserialize)]
struct UserClaims {
    sub: String,           // User ID
    sui_address: String,   // User's Sui address
    exp: u64,             // Expiration
}
```

#### **Option 3: Delegated Signing Service**

Separate wallet management from the gateway:

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   Client    │    │   Gateway   │    │   Wallet    │
│             │───▶│             │───▶│   Service   │
│             │    │             │    │             │
└─────────────┘    └─────────────┘    └─────────────┘
```

### 🔧 **Quick Fix: Environment-Based Credentials**

For development/testing, we can improve the current approach:

```toml
# config.toml
[credential_mapping]
# Map S3 access keys to external wallet configs
alice = { sui_address = "0x...", keystore_path = "/secure/alice.keystore" }
bob = { sui_address = "0x...", keystore_path = "/secure/bob.keystore" }

# Or use environment variables
[credential_mapping]
alice = { sui_address = "${ALICE_SUI_ADDRESS}", keystore_env = "ALICE_KEYSTORE" }
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Implement your changes with tests
4. Submit a pull request with a clear description

## License

This project is licensed under the Apache 2.0 License - see the `LICENSE` file for details.

---

**Note**: This implementation provides a complete S3-compatible interface to Walrus storage with **full read-write functionality**. The current architecture demonstrates the feasibility while highlighting critical security considerations for production deployment.

### 🚨 **Production Deployment Checklist**

Before deploying to production, implement one of the secure credential strategies:

- [ ] **Client-Side Signing**: Users sign transactions locally
- [ ] **JWT Authentication**: External auth system with service account  
- [ ] **Delegated Signing**: Hardened wallet service with HSM
- [ ] **Environment Credentials**: Move away from direct keystore storage

### 🎯 **Project Status**

- ✅ **S3 API Compatibility**: Complete implementation
- ✅ **Authentication**: SigV4 with credential extraction
- ✅ **Read Operations**: Efficient shared client
- ✅ **Write Operations**: Per-request client creation
- ✅ **Security Framework**: Multiple credential strategies
- ⚠️ **Production Ready**: Requires secure credential strategy

The gateway is **functional and feature-complete** but requires security hardening for production use!
