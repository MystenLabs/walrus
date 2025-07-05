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

The easiest way to test the Walrus S3 Gateway with client-side signing is to use the comprehensive test script:

```bash
# 1. Start the gateway in one terminal
cargo run --bin walrus-s3-gateway -- --config test-config.toml

# 2. Run the complete test in another terminal
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

The default test configuration is in `test-config.toml`. You can modify it for your specific needs:

```toml
# Basic configuration for testing
listen_address = "127.0.0.1:9200"
walrus_config_file = "client_config.yaml"
sui_config_file = "~/.sui/sui_config/client.yaml"
```

## Troubleshooting

- **Gateway not accessible**: Make sure it's running with `cargo run --bin walrus-s3-gateway -- --config test-config.toml`
- **Sui CLI not found**: Follow the installation guide at https://docs.sui.io/build/install
- **Faucet timeout**: This is normal in test environments, the script will continue
- **Address extraction failed**: The script includes multiple fallback methods for robustness

## Next Steps

- Read the [CLIENT-SIDE-SIGNING.md](CLIENT-SIDE-SIGNING.md) for detailed information about the client-side signing feature
- Check the [IMPLEMENTATION-SUMMARY.md](IMPLEMENTATION-SUMMARY.md) for technical details
- Explore the gateway's API endpoints manually using the examples in the README
