# Test Script Usage

## test-complete.sh

Complete end-to-end testing script for Walrus S3 Gateway with client-side signing.

### What it does:

1. **🔍 Dependency Check**: Verifies Sui CLI, curl, and optional jq are installed
2. **🔐 Wallet Creation**: Creates temporary Sui wallet with testnet funds
3. **🔗 Gateway Check**: Verifies the gateway is running and accessible
4. **🧪 Comprehensive Testing**: 
   - List buckets operation
   - PUT object with client-side signing requirement (HTTP 202)
   - Transaction template generation
   - Signed transaction submission  
   - GET and DELETE object operations
5. **🧹 Automatic Cleanup**: Removes temporary wallet and test files

### Prerequisites:

- Sui CLI installed and accessible in PATH
- Gateway running at http://127.0.0.1:9200
- Internet connection for testnet faucet

### Usage:

```bash
# Start the gateway (in one terminal)
cargo run --bin walrus-s3-gateway -- --config test-config.toml

# Run tests (in another terminal)  
./test-complete.sh
```

### Expected Output:

- Green checkmarks for successful operations
- HTTP 202 responses for PUT operations requiring client signing
- Transaction templates and submission responses
- Comprehensive test summary

The script automatically handles wallet creation, funding, and cleanup, providing a complete testing experience without manual setup.
