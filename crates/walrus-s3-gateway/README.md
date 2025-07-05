# Walrus S3 Gateway

A high-performance S3-compatible gateway for Walrus storage system with client-side signing capabilities.

## Table of Contents

1. [Overview](#overview)
2. [Features](#features)
3. [Quick Start](#quick-start)
4. [Architecture](#architecture)
5. [Configuration](#configuration)
6. [Client-Side Signing](#client-side-signing)
7. [API Reference](#api-reference)
8. [Testing](#testing)
9. [Troubleshooting](#troubleshooting)
10. [Implementation Details](#implementation-details)

## Overview

The Walrus S3 Gateway provides an S3-compatible interface to the Walrus decentralized storage network. It enables developers to use familiar S3 APIs while benefiting from Walrus's robust, distributed storage capabilities.

The gateway implements client-side signing for secure authentication using SUI-based signatures, ensuring that only authorized clients can access the storage system.

## Features

- **S3 Compatible API**: Full compatibility with standard S3 operations (GetObject, PutObject, ListObjects)
- **Client-Side Signing**: Secure authentication with SUI-based signatures
- **High Performance**: Optimized for concurrent operations with configurable timeouts
- **Configurable**: Flexible configuration options for different deployment scenarios
- **CORS Support**: Built-in CORS handling for web applications
- **Permission Management**: Fine-grained access control with read/write permissions
- **TLS Support**: Optional TLS encryption for secure communications

## Quick Start### Prerequisites

- **Rust**: Install from [rustup.rs](https://rustup.rs/)
- **Sui CLI**: Install from [Sui docs](https://docs.sui.io/build/install)
- **curl**: For HTTP requests (usually pre-installed)
- **jq** (optional): For JSON formatting (`brew install jq` on macOS)

### 1. Build the Gateway

```bash
# Clone the repository if not already done
git clone <repository-url>
cd walrus/crates/walrus-s3-gateway

# Build the gateway
cargo build --release
```

### 2. Configure the Gateway

The gateway uses a `config.toml` file located in the crate directory. Edit this file to match your setup:```toml# Basic server configurationaccess_key = "your-access-key"secret_key = "your-secret-key"bind_address = "127.0.0.1:9200"region = "us-east-1"enable_tls = falsewalrus_config_path = "client_config.yaml"request_timeout = 300max_body_size = 67108864  # 64MBenable_cors = true# Client-side signing configuration[client_signing]require_signatures = truevalidate_signatures = truesui_rpc_url = "https://fullnode.testnet.sui.io:443"# Walrus configuration[walrus]publisher_url = "https://publisher.walrus-testnet.walrus.space"aggregator_url = "https://aggregator.walrus-testnet.walrus.space"# Client credentials[client_credentials]"your-access-key" = {     sui_address = "0x1234567890abcdef...",     permissions = ["read", "write"],     description = "Production client",     active = true }```### 3. Set Up Walrus Client Configuration

Create a `client_config.yaml` file in the crate directory (copy from `../../setup/client_config.yaml`):

```yaml
# Walrus client configuration
# Configure according to your Walrus network setup
```

### 4. Run the Gateway

```bash
# Run from the project root
./target/release/walrus-s3-gateway

# Or run with cargo
cargo run --release --bin walrus-s3-gateway

# With custom configuration
cargo run --release --bin walrus-s3-gateway -- --config my-config.toml
```

The gateway will automatically load the configuration from the crate directory and start serving on the configured address.

**Note**: The gateway requires connectivity to Walrus testnet services and may experience startup delays.

### 5. Test the Connection

```bash
# Basic connectivity test
curl -X GET http://127.0.0.1:9200/

# Test client-side signing workflow
./test-complete.sh
```

## Architecture

The Walrus S3 Gateway consists of several key components:### Core Components

1. **HTTP Server**: Handles incoming S3 API requests
2. **Authentication Layer**: Validates client credentials and signatures
3. **Request Handler**: Processes S3 operations and translates them to Walrus operations
4. **Walrus Client**: Interfaces with the underlying Walrus storage network
5. **Configuration Manager**: Loads and manages gateway configuration

### Request Flow

1. **Client Request**: S3-compatible client sends request to gateway
2. **Authentication**: Gateway validates client credentials and signatures
3. **Authorization**: Gateway checks client permissions for requested operation
4. **Translation**: S3 request is translated to Walrus operation
5. **Storage Operation**: Operation is executed on Walrus network
6. **Response**: Result is translated back to S3 response format

### Security Model

The gateway implements a multi-layered security model:

- **Client Authentication**: SUI-based signatures for client identity verification
- **Permission-Based Authorization**: Fine-grained access control per client
- **Request Validation**: Comprehensive validation of all incoming requests
- **Signature Verification**: Cryptographic verification of request signatures

## Configuration

### Configuration File Location

The gateway automatically loads its configuration from `config.toml` in the crate directory (`crates/walrus-s3-gateway/config.toml`). This behavior is consistent regardless of the working directory from which the gateway is started.### Configuration Sections

#### Server Settings

```toml
# Server binding configuration
bind_address = "127.0.0.1:9200"  # IP and port to bind to
region = "us-east-1"              # AWS region identifier
enable_tls = false                # Enable TLS encryption
enable_cors = true                # Enable CORS headers

# Request handling
request_timeout = 300             # Request timeout in seconds
max_body_size = 67108864         # Maximum request body size (64MB)
```

#### Authentication

```toml
# Default credentials (for backward compatibility)
access_key = "default-access-key"
secret_key = "default-secret-key"
```

#### Walrus Backend

```toml
# Path to Walrus client configuration
walrus_config_path = "client_config.yaml"

# Walrus endpoints
[walrus]
publisher_url = "https://publisher.walrus-testnet.walrus.space"
aggregator_url = "https://aggregator.walrus-testnet.walrus.space"
```

#### Client-Side Signing

```toml
[client_signing]
require_signatures = true                                    # Require client signatures
validate_signatures = true                                  # Validate signature authenticity
sui_rpc_url = "https://fullnode.testnet.sui.io:443"        # SUI RPC endpoint
```

#### Client Credentials

```toml
[client_credentials]
"client-1" = {
    sui_address = "0x1234...",
    permissions = ["read", "write"],
    description = "Full access client",
    active = true
}
"client-2" = {
    sui_address = "0x5678...",
    permissions = ["read"],
    description = "Read-only client",
    active = true
}
```

**Client Credential Fields:**
- `sui_address`: The SUI address associated with the client's signing key
- `permissions`: Array of permissions (`read`, `write`)
- `description`: Human-readable description of the credential
- `active`: Boolean flag to enable/disable the credential

## Client-Side Signing

The Walrus S3 Gateway implements client-side signing for secure authentication using SUI-based cryptographic signatures.### How It Works

The gateway uses a unique client-side signing workflow:

1. **S3 PUT Request**: Client sends standard S3 PUT operation
2. **Transaction Template**: Gateway responds with HTTP 202 + unsigned transaction template
3. **Local Signing**: Client signs transaction with their SUI wallet
4. **Transaction Submission**: Client submits signed transaction via dedicated endpoint
5. **Execution**: Gateway validates and executes on Walrus network

```
Client ──PUT──> Gateway ──202+Template──> Client ──Sign──> Wallet
  ↑                                                          │
  └──────────── Submit Signed TX ←─────────────────────────┘
```

### Key Benefits

- **Private Key Protection**: Keys never leave client devices
- **Transaction Transparency**: Clients see exactly what they're signing
- **User Control**: Full control over blockchain transactions
- **Validation Only**: Gateway only validates, never creates signatures

### Signature Format

The gateway supports SUI-compatible signatures in the following format:

```
Authorization: AWS4-HMAC-SHA256 Credential=<access_key>/<date>/<region>/s3/aws4_request, SignedHeaders=<headers>, Signature=<signature>
```
### Required Headers

For signed requests, the following headers must be present:

- `Authorization`: Contains the signature and metadata
- `X-Amz-Date`: Timestamp of the request
- `X-Amz-Content-Sha256`: SHA256 hash of the request body
- `Host`: Target host header

### Example Client Integration

```javascript
// Handle PUT response
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

## API Reference

The gateway implements the following S3-compatible API endpoints:

### Bucket Operations

#### GET / (ListBuckets)
List all buckets accessible to the client.

```bash
GET /
Host: 127.0.0.1:9200
Authorization: AWS4-HMAC-SHA256 ...
```

Response:
```xml
<?xml version="1.0" encoding="UTF-8"?>
<ListAllMyBucketsResult>
    <Owner>
        <ID>client-id</ID>
        <DisplayName>Client Name</DisplayName>
    </Owner>
    <Buckets>
        <Bucket>
            <Name>bucket-name</Name>
            <CreationDate>2024-01-01T00:00:00Z</CreationDate>
        </Bucket>
    </Buckets>
</ListAllMyBucketsResult>
```

#### GET /{bucket} (ListObjects)
List objects in a specific bucket.

```bash
GET /bucket-name
Host: 127.0.0.1:9200
Authorization: AWS4-HMAC-SHA256 ...
```

Query Parameters:
- `prefix`: Filter objects by prefix
- `delimiter`: Delimiter for grouping
- `max-keys`: Maximum number of objects to return
- `marker`: Pagination marker

### Object Operations

#### GET /{bucket}/{key} (GetObject)
Retrieve an object from storage.

```bash
GET /bucket-name/object-key
Host: 127.0.0.1:9200
Authorization: AWS4-HMAC-SHA256 ...
```

Response: Object data with appropriate headers.

#### PUT /{bucket}/{key} (PutObject)
Store an object in storage. For client-side signing, returns HTTP 202 with transaction template.

```bash
PUT /bucket-name/object-key
Host: 127.0.0.1:9200
Authorization: AWS4-HMAC-SHA256 ...
Content-Type: application/octet-stream
Content-Length: 1024

[object data]
```

Response (202):
```json
{
  "transaction_template": "<unsigned_transaction>",
  "bucket": "bucket-name",
  "key": "object-key",
  "signing_instructions": "Sign with your SUI wallet"
}
```

#### DELETE /{bucket}/{key} (DeleteObject)
Delete an object from storage.

```bash
DELETE /bucket-name/object-key
Host: 127.0.0.1:9200
Authorization: AWS4-HMAC-SHA256 ...
```

#### HEAD /{bucket}/{key} (HeadObject)
Get object metadata without retrieving the object data.

```bash
HEAD /bucket-name/object-key
Host: 127.0.0.1:9200
Authorization: AWS4-HMAC-SHA256 ...
```

### Client-Side Signing Endpoints

#### POST /_walrus/generate-transaction
Generate transaction templates for client-side signing.

#### POST /_walrus/submit-transaction
Submit signed transactions to the Walrus network.

### Error Responses

The gateway returns standard S3 error responses:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<Error>
    <Code>NoSuchBucket</Code>
    <Message>The specified bucket does not exist</Message>
    <BucketName>bucket-name</BucketName>
    <RequestId>request-id</RequestId>
</Error>
```

Common error codes:
- `NoSuchBucket`: Bucket does not exist
- `NoSuchKey`: Object does not exist
- `AccessDenied`: Client lacks permission
- `InvalidAccessKeyId`: Unknown access key
- `SignatureDoesNotMatch`: Invalid signature

## Testing

The gateway includes comprehensive testing utilities and configurations.

### Test Configuration Files

- `test-config.toml`: Full test configuration with all features enabled
- `test-config-minimal.toml`: Minimal test configuration for basic functionality testing

### Running Tests

#### Automated Testing

```bash
# Run the complete test suite
./test-complete.sh

# This script will:
# 1. Create a temporary Sui wallet with testnet funds
# 2. Test all S3 endpoints with real client-side signing workflow
# 3. Validate HTTP 202 responses for PUT operations
# 4. Test transaction template generation and submission
# 5. Display comprehensive test results
# 6. Clean up automatically
```

#### Unit Tests

```bash
# Run all unit tests
cargo test

# Run tests for specific module
cargo test config
cargo test handlers
cargo test credentials
```

#### Integration Tests

```bash
# Run integration tests
cargo test --test integration

# Run with specific test configuration
WALRUS_CONFIG_PATH=test-config.toml cargo test --test integration
```

### Manual Testing

#### Basic Connectivity Test

```bash
# Start the gateway
cargo run --release --bin walrus-s3-gateway

# Test basic connectivity
curl -v http://127.0.0.1:9200/

# Expected response: HTTP 200 with ListBuckets XML
```

#### Authentication Test

```bash
# Test with valid credentials
curl -X GET http://127.0.0.1:9200/ \
  -H "Authorization: AWS4-HMAC-SHA256 Credential=test-access-key/20240101/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=..." \
  -H "X-Amz-Date: 20240101T000000Z" \
  -H "Host: 127.0.0.1:9200"

# Test with invalid credentials
curl -X GET http://127.0.0.1:9200/ \
  -H "Authorization: AWS4-HMAC-SHA256 Credential=invalid-key/20240101/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=..." \
  -H "X-Amz-Date: 20240101T000000Z" \
  -H "Host: 127.0.0.1:9200"
```

#### Object Operations Test

```bash
# Upload an object (will return 202 with transaction template)
curl -X PUT http://127.0.0.1:9200/test-bucket/test-object \
  -H "Authorization: AWS4-HMAC-SHA256 ..." \
  -H "X-Amz-Date: ..." \
  -H "X-Amz-Content-Sha256: ..." \
  -H "Content-Type: text/plain" \
  --data "Hello, Walrus!"

# Download the object
curl -X GET http://127.0.0.1:9200/test-bucket/test-object \
  -H "Authorization: AWS4-HMAC-SHA256 ..." \
  -H "X-Amz-Date: ..."

# Delete the object
curl -X DELETE http://127.0.0.1:9200/test-bucket/test-object \
  -H "Authorization: AWS4-HMAC-SHA256 ..." \
  -H "X-Amz-Date: ..."
```

## Troubleshooting

### Common Issues

#### Gateway Not Starting

**Symptoms**: Gateway appears to hang during startup

**Common Causes**:
1. **Missing configuration files**: Ensure both `config.toml` and `client_config.yaml` exist
2. **Walrus network connectivity**: The gateway needs to connect to Walrus testnet services
3. **Sui RPC connectivity**: Verify connection to Sui testnet RPC endpoint

**Solutions**:
- Check configuration file paths
- Verify network connectivity
- Check console output for specific error messages

#### Configuration Not Found

**Error**: `Failed to load configuration: No such file or directory`

**Solution**: Ensure `config.toml` exists in the crate directory (`crates/walrus-s3-gateway/config.toml`).

#### Gateway Not Accessible

**Symptoms**: Gateway fails to respond to requests

**Solutions**:
1. Check that it bound to the correct address (`127.0.0.1:9200`)
2. Verify no other service is using port 9200
3. Check firewall settings
4. Review console output for binding errors

#### Invalid Signature

**Error**: `SignatureDoesNotMatch`

**Solutions**:
1. Verify the SUI address in client credentials matches the signing key
2. Check that the signature algorithm is correct
3. Ensure all required headers are present and correctly formatted
4. Verify the timestamp is within acceptable range

#### Permission Denied

**Error**: `AccessDenied`

**Solutions**:
1. Check that the client has the required permissions (`read` or `write`)
2. Verify the client credential is marked as `active = true`
3. Ensure the access key exists in the `[client_credentials]` section

#### Test Script Issues

**Symptoms**: `./test-complete.sh` fails

**Solutions**:
1. Ensure Sui CLI is installed and working: `sui --version`
2. Check that curl and jq are available
3. Verify internet connectivity for testnet faucet
4. Check that the gateway is running and accessible

#### Walrus Backend Issues

**Error**: `Failed to connect to Walrus backend`

**Solutions**:
1. Verify `walrus_config_path` points to a valid configuration file
2. Check Walrus network connectivity
3. Ensure Walrus client configuration is correct
4. Verify Walrus endpoint URLs are accessible

### Debug Mode

Enable debug logging for detailed troubleshooting:

```bash
# Set environment variable for debug logging
RUST_LOG=debug cargo run --release --bin walrus-s3-gateway

# Or with specific module logging
RUST_LOG=walrus_s3_gateway=debug cargo run --release --bin walrus-s3-gateway
```

### Performance Tuning

#### Request Timeout

Adjust the request timeout based on your network conditions:

```toml
request_timeout = 600  # 10 minutes for slow networks
```

#### Body Size Limits

Configure maximum body size for large file uploads:

```toml
max_body_size = 134217728  # 128MB for large files
```

#### Concurrent Connections

The gateway uses async processing to handle multiple concurrent connections efficiently. Monitor system resources and adjust as needed.

## Implementation Details

### Technical Architecture

The Walrus S3 Gateway is built using:

- **Rust**: High-performance, memory-safe systems programming
- **Tokio**: Async runtime for concurrent request handling
- **Hyper**: HTTP server implementation
- **Serde**: Serialization/deserialization for configuration and data
- **SUI SDK**: Integration with SUI blockchain for signature verification

### Key Modules

#### `src/bin/walrus-s3-gateway.rs`
Main entry point that:
- Loads configuration from the crate directory
- Initializes the HTTP server
- Sets up request routing
- Handles graceful shutdown

#### `src/config.rs`
Configuration management:
- Loads and validates configuration from `config.toml`
- Provides type-safe configuration access
- Handles environment variable overrides

#### `src/credentials.rs`
Client credential management:
- Stores and validates client credentials
- Manages permission checking
- Handles credential activation/deactivation

#### `src/handlers/mod.rs`
Request handlers:
- Implements S3 API endpoints
- Handles request parsing and validation
- Manages response formatting

#### `src/handlers/signing.rs`
Client-side signing handlers:
- Transaction template generation
- Signed transaction validation
- Error handling for signing failures

### Security Implementation

The gateway implements several security measures:

1. **Signature Verification**: All requests are verified against SUI blockchain signatures
2. **Permission Checking**: Fine-grained access control based on client permissions
3. **Input Validation**: Comprehensive validation of all input parameters
4. **Rate Limiting**: Protection against abuse (configurable)
5. **Error Handling**: Secure error responses that don't leak sensitive information

### Performance Optimizations

1. **Async Processing**: Non-blocking I/O for handling multiple concurrent requests
2. **Connection Pooling**: Efficient reuse of connections to Walrus backend
3. **Request Pipelining**: Batched operations where possible
4. **Caching**: Strategic caching of frequently accessed data
5. **Memory Management**: Efficient memory usage with zero-copy operations where possible

### Future Enhancements

Planned improvements include:

1. **Enhanced Monitoring**: Metrics and telemetry for production deployments
2. **Advanced Caching**: Distributed caching for improved performance
3. **Multi-Region Support**: Geographic distribution of gateway instances
4. **Backup and Recovery**: Automated backup and disaster recovery features
5. **API Extensions**: Additional S3-compatible APIs and custom extensions
6. **Transaction Template Caching**: Implement caching for transaction templates
7. **Production Features**: Add comprehensive TLS, monitoring, and logging

## Next Steps

1. **Integration**: Test with real Sui wallets and production environments
2. **Performance**: Implement transaction template caching and optimize request handling
3. **Features**: Add comprehensive GET, DELETE, LIST operations with client-side signing
4. **Production**: Add TLS, comprehensive monitoring, and production-ready logging
5. **Documentation**: Create integration guides for popular S3 client libraries

---

For more information, issues, or contributions, please refer to the main project repository and documentation.
