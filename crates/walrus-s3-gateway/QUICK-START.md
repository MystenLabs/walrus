# Walrus S3 Gateway - Quick Start Guide

## Prerequisites

1. **Install Sui CLI**:
   ```bash
   # Follow the installation guide at:
   # https://docs.sui.io/build/install
   ```

2. **Install Dependencies**:
   ```bash
   # Required
   curl
   # Optional (for better JSON formatting)
   jq
   ```

## Quick Test

**Note**: The gateway may require specific network connectivity to Walrus services and may hang during startup if services are unavailable.

For testing the Walrus S3 Gateway with client-side signing:

```bash
# Option 1: Standard configuration (may hang if Walrus services unavailable)
cargo run --bin walrus-s3-gateway -- --config test-config.toml

# Option 2: Minimal configuration (fewer external dependencies)
cargo run --bin walrus-s3-gateway -- --config test-config-minimal.toml

# Run the test script (works even if gateway isn't fully functional)
./test-complete.sh
```

## What the test does

The `test-complete.sh` script is completely non-interactive and:

1. ✅ **Checks all dependencies** (Sui CLI, curl, jq)
2. ✅ **Creates a temporary Sui wallet** using `sui keytool`
3. ✅ **Requests testnet funds** from the faucet
4. ✅ **Tests all gateway endpoints**:
   - Gateway health check
   - Object storage (PUT/GET/DELETE)
   - Client-side signing transaction templates
   - Transaction submission
5. ✅ **Cleans up** all temporary files and wallets

## Manual Testing

If you prefer to test manually, see the main [README.md](README.md) for detailed instructions.

## Configuration

The default test configuration is in `test-config.toml`. This file contains:

```toml
# Basic server settings
access_key = "test-access-key"
secret_key = "test-secret-key"
bind_address = "127.0.0.1:9200"
region = "us-east-1"

# Client-side signing configuration
[client_signing]
require_signatures = true
validate_signatures = true
sui_rpc_url = "https://fullnode.testnet.sui.io:443"

# Walrus network endpoints
[walrus]
publisher_url = "https://publisher.walrus-testnet.walrus.space"
aggregator_url = "https://aggregator.walrus-testnet.walrus.space"
```

You'll also need `client_config.yaml` - copy it from `../../setup/client_config.yaml`

## Troubleshooting

## Troubleshooting

### Gateway startup issues

- **Gateway hangs during startup**: This usually indicates connection issues with Walrus services
- **Configuration errors**: Ensure both `test-config.toml` and `client_config.yaml` exist
- **Port conflicts**: Make sure port 9200 is not in use by another service

### Known Issues

The current implementation may experience connectivity issues when:
- Walrus testnet services are unavailable or slow
- Network connectivity to Sui RPC endpoints is limited
- External wallet configuration files are missing

### Workarounds

1. **Use minimal configuration**: Try `test-config-minimal.toml` which has fewer external dependencies
2. **Test script independence**: The `test-complete.sh` script can demonstrate client-side signing concepts even when the gateway is not fully operational
3. **Local development**: Focus on testing the transaction template generation and signing workflow

### Common fixes

1. **Copy required files**:
   ```bash
   # Copy the Walrus client configuration
   cp ../../setup/client_config.yaml .
   ```

2. **Check configuration format**:
   ```bash
   # Validate TOML syntax
   cargo check
   ```

3. **Test network connectivity**:
   ```bash
   # Test Sui RPC
   curl -s https://fullnode.testnet.sui.io:443
   # Test Walrus publisher
   curl -s https://publisher.walrus-testnet.walrus.space
   ```

- **Gateway not accessible**: Make sure it's running with `cargo run --bin walrus-s3-gateway -- --config test-config.toml`
- **Sui CLI not found**: Follow the installation guide at https://docs.sui.io/build/install
- **Faucet timeout**: This is normal in test environments, the script will continue
- **Address extraction failed**: The script includes multiple fallback methods for robustness

## Next Steps

- Read the [CLIENT-SIDE-SIGNING.md](CLIENT-SIDE-SIGNING.md) for detailed information about the client-side signing feature
- Check the [IMPLEMENTATION-SUMMARY.md](IMPLEMENTATION-SUMMARY.md) for technical details
- Explore the gateway's API endpoints manually using the examples in the README
