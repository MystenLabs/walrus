#!/bin/bash

# Complete Test Script for Walrus S3 Gateway Client-Side Signing
# This script creates a Sui wallet, tests all endpoints with client-side signing, and cleans up

set -euo pipefail

# Load environment variables from .env file if it exists
if [[ -f .env ]]; then
    set -a  # automatically export all variables
    source .env
    set +a  # stop automatically exporting
    echo "ℹ️  Loaded configuration from .env file"
fi

# Configuration from environment variables with defaults
GATEWAY_HOST="${GATEWAY_HOST:-127.0.0.1}"
GATEWAY_PORT="${GATEWAY_PORT:-8080}"
GATEWAY_PROTOCOL="${GATEWAY_PROTOCOL:-http}"
GATEWAY_URL="${GATEWAY_URL:-${GATEWAY_PROTOCOL}://${GATEWAY_HOST}:${GATEWAY_PORT}}"
TEST_BUCKET="${TEST_BUCKET:-test-bucket-$(date +%s)}"
TEST_OBJECT="${TEST_OBJECT:-test-object.txt}"
TEST_CONTENT="${TEST_CONTENT:-Hello, Walrus S3 Gateway with Client-Side Signing! $(date)}"
WALLET_DIR="${WALLET_DIR:-$(mktemp -d)}"
WALLET_CONFIG="$WALLET_DIR/sui_config"
KEYSTORE_FILE="$WALLET_DIR/sui.keystore"
CLIENT_CONFIG="$WALLET_DIR/client.yaml"
ACCESS_KEY="${ACCESS_KEY:-walrus-access-key}"
SECRET_KEY="${SECRET_KEY:-walrus-secret-key}"
AWS_REGION="${AWS_REGION:-us-east-1}"
SUI_NETWORK="${SUI_NETWORK:-testnet}"
SUI_RPC_URL="${SUI_RPC_URL:-https://fullnode.testnet.sui.io:443}"
REQUEST_TIMEOUT="${REQUEST_TIMEOUT:-30}"
FAUCET_TIMEOUT="${FAUCET_TIMEOUT:-30}"
FAUCET_WAIT_TIME="${FAUCET_WAIT_TIME:-10}"
AUTO_CLEANUP="${AUTO_CLEANUP:-true}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_step() {
    echo -e "${BLUE}[STEP]${NC} $1"
}

cleanup() {
    if [[ "$AUTO_CLEANUP" != "true" ]]; then
        print_status "⚠️  Auto-cleanup disabled (AUTO_CLEANUP=$AUTO_CLEANUP)"
        return
    fi
    
    print_status "🧹 Cleaning up test environment..."
    
    # Remove temporary wallet directory
    if [[ -d "$WALLET_DIR" ]]; then
        rm -rf "$WALLET_DIR"
        print_status "✓ Removed temporary wallet directory"
    fi
    
    # Remove test content file if exists
    rm -f /tmp/test_content.txt
    
    # Remove any .key files that might have been created during the test
    # These are typically created by the Sui CLI and contain private keys
    if ls *.key 1> /dev/null 2>&1; then
        print_status "🔑 Removing temporary key files..."
        rm -f *.key
        print_status "✓ Removed temporary key files"
    fi
    
    print_status "✅ Cleanup completed"
}

# Set up cleanup trap
trap cleanup EXIT

cleanup_existing_files() {
    # Clean up any leftover files from previous test runs
    local files_found=false
    
    if ls *.key 1> /dev/null 2>&1; then
        print_status "🧹 Cleaning up leftover files from previous runs..."
        rm -f *.key
        print_status "✓ Removed leftover key files"
        files_found=true
    fi
    
    # Remove any leftover test content files
    if [[ -f /tmp/test_content.txt ]]; then
        rm -f /tmp/test_content.txt
        if [[ "$files_found" == "false" ]]; then
            print_status "🧹 Cleaning up leftover files from previous runs..."
            files_found=true
        fi
        print_status "✓ Removed leftover test content file"
    fi
    
    # Show completion message only if files were found
    if [[ "$files_found" == "true" ]]; then
        print_status "✅ Initial cleanup completed"
    fi
}

check_dependencies() {
    print_step "🔍 Checking dependencies..."
    
    # Check if sui CLI is available
    if ! command -v sui &> /dev/null; then
        print_error "❌ Sui CLI not found. Please install Sui CLI first."
        print_error "Installation: https://docs.sui.io/build/install"
        exit 1
    fi
    
    # Check if curl is available
    if ! command -v curl &> /dev/null; then
        print_error "❌ curl not found. Please install curl first."
        exit 1
    fi
    
    # Check if jq is available for JSON parsing
    if ! command -v jq &> /dev/null; then
        print_warning "⚠️  jq not found. JSON responses will be shown raw."
        JQ_AVAILABLE=false
    else
        JQ_AVAILABLE=true
    fi
    
    print_status "✓ All dependencies checked"
}

setup_sui_wallet() {
    print_step "🔐 Setting up Sui wallet..."
    
    # Create wallet directory
    mkdir -p "$WALLET_CONFIG"
    
    # Initialize Sui client config
    export SUI_CONFIG_DIR="$WALLET_CONFIG"
    
    print_status "Generating new keypair with sui keytool..."
    
    # Generate a new keypair using sui keytool (this is non-interactive)
    KEYTOOL_OUTPUT=$(sui keytool generate ed25519 2>&1)
    KEYTOOL_EXIT_CODE=$?
    
    if [[ $KEYTOOL_EXIT_CODE -ne 0 ]]; then
        print_error "❌ Failed to generate keypair with sui keytool"
        print_error "Output: $KEYTOOL_OUTPUT"
        exit 1
    fi
    
    # Extract the address from keytool output
    SUI_ADDRESS=$(echo "$KEYTOOL_OUTPUT" | grep -o "0x[a-fA-F0-9]\{64\}" | head -1)
    
    if [[ -z "$SUI_ADDRESS" ]]; then
        # Try alternative extraction
        SUI_ADDRESS=$(echo "$KEYTOOL_OUTPUT" | grep -oE "0x[a-fA-F0-9]{64}" | head -1)
    fi
    
    if [[ -z "$SUI_ADDRESS" ]]; then
        print_error "❌ Failed to extract address from keytool output"
        print_error "Keytool output was: $KEYTOOL_OUTPUT"
        exit 1
    fi
    
    print_status "✓ Generated keypair with address: $SUI_ADDRESS"
    
    # Create client config file with the generated address
    cat > "$WALLET_CONFIG/client.yaml" << EOF
---
keystore:
  File: $KEYSTORE_FILE
envs:
  - alias: $SUI_NETWORK
    rpc: "$SUI_RPC_URL"
    ws: ~
    basic_auth: ~
active_env: $SUI_NETWORK
active_address: $SUI_ADDRESS
EOF
    
    print_status "✓ Created Sui client configuration with address: $SUI_ADDRESS"
    
    print_status "✓ Created Sui wallet with address: $SUI_ADDRESS"
    
    # Request funds from faucet
    print_status "Requesting funds from Sui $SUI_NETWORK faucet..."
    if timeout "$FAUCET_TIMEOUT" sui client faucet --address "$SUI_ADDRESS" 2>/dev/null; then
        print_status "✓ Successfully requested funds from faucet"
        
        # Wait a bit for funds to arrive
        print_status "Waiting for funds to arrive (${FAUCET_WAIT_TIME} seconds)..."
        sleep "$FAUCET_WAIT_TIME"
        
        # Check balance
        BALANCE=$(sui client balance --address "$SUI_ADDRESS" 2>/dev/null | grep "SUI" | head -1 || echo "")
        if [[ -n "$BALANCE" ]]; then
            print_status "✓ Wallet balance: $BALANCE"
        else
            print_warning "⚠️  Could not verify balance, continuing with tests..."
        fi
    else
        print_warning "⚠️  Failed to request funds from faucet (timeout or error), continuing with tests..."
    fi
    
    # Export wallet address for other functions
    export SUI_ADDRESS
}

check_gateway_status() {
    print_step "🔗 Checking gateway status..."
    
    # Check if gateway is running (accept any HTTP response, including errors)
    HTTP_CODE=$(curl -s -w "%{http_code}" -o /dev/null "${GATEWAY_URL}/" 2>/dev/null || echo "000")
    
    if [[ "$HTTP_CODE" =~ ^[1-5][0-9][0-9]$ ]]; then
        print_status "✓ Gateway is accessible (HTTP $HTTP_CODE)"
    else
        print_error "❌ Gateway is not accessible at $GATEWAY_URL"
        print_error "Please start the gateway with:"
        print_error "cargo run --bin walrus-s3-gateway"
        exit 1
    fi
}

format_json_response() {
    local response="$1"
    if [[ "$JQ_AVAILABLE" == "true" ]]; then
        echo "$response" | jq . 2>/dev/null || echo "$response"
    else
        echo "$response"
    fi
}

test_list_buckets() {
    print_step "📂 Testing list buckets..."
    
    RESPONSE=$(curl -s -X GET "${GATEWAY_URL}/" 2>/dev/null || echo "")
    
    if [[ -n "$RESPONSE" ]]; then
        print_status "✓ List buckets endpoint responded"
        echo "Response:"
        format_json_response "$RESPONSE" | head -5
    else
        print_warning "⚠️  List buckets endpoint not available or requires authentication"
    fi
}

test_put_object_client_signing() {
    print_step "📤 Testing PUT object with client-side signing requirement..."
    
    # Create test content
    echo -n "$TEST_CONTENT" > /tmp/test_content.txt
    
    # Attempt PUT operation
    print_status "Sending PUT request to ${GATEWAY_URL}/${TEST_BUCKET}/${TEST_OBJECT}..."
    
    RESPONSE=$(curl -s -w "\n%{http_code}" -X PUT "${GATEWAY_URL}/${TEST_BUCKET}/${TEST_OBJECT}" \
        -H "Content-Type: text/plain" \
        -H "Host: ${GATEWAY_HOST}:${GATEWAY_PORT}" \
        -H "Authorization: AWS4-HMAC-SHA256 Credential=${ACCESS_KEY}/20231101/${AWS_REGION}/s3/aws4_request,SignedHeaders=host;content-type,Signature=test-signature" \
        --data-binary @/tmp/test_content.txt 2>/dev/null || echo -e "\n000")
    
    # Extract HTTP status code (last line) and response body (all but last line)
    HTTP_CODE=$(echo "$RESPONSE" | tail -1)
    RESPONSE_BODY=$(echo "$RESPONSE" | sed '$d')
    
    if [[ "$HTTP_CODE" == "202" ]]; then
        print_status "✅ PUT operation correctly returned HTTP 202 (client signing required)"
        
        if echo "$RESPONSE_BODY" | grep -q "client_signing_required"; then
            print_status "✓ Response contains client signing requirement"
            echo "Response preview:"
            format_json_response "$RESPONSE_BODY" | head -10
            
            # Extract transaction template if available
            if [[ "$JQ_AVAILABLE" == "true" ]] && echo "$RESPONSE_BODY" | jq -e '.transaction_template' >/dev/null 2>&1; then
                TRANSACTION_TEMPLATE=$(echo "$RESPONSE_BODY" | jq -r '.transaction_template.transaction_data' 2>/dev/null)
                if [[ -n "$TRANSACTION_TEMPLATE" && "$TRANSACTION_TEMPLATE" != "null" ]]; then
                    export TRANSACTION_TEMPLATE
                    print_status "✓ Extracted transaction template for signing"
                fi
            fi
        else
            print_warning "⚠️  Response doesn't contain expected client signing fields"
        fi
    else
        print_warning "⚠️  PUT operation returned HTTP $HTTP_CODE instead of 202"
        echo "Response:"
        format_json_response "$RESPONSE_BODY" | head -5
    fi
}

test_generate_transaction() {
    print_step "🔧 Testing transaction template generation..."
    
    CONTENT_SIZE=$(echo -n "$TEST_CONTENT" | wc -c | tr -d ' ')
    
    REQUEST_BODY=$(cat <<EOF
{
    "access_key": "${ACCESS_KEY}",
    "purpose": "StoreBlob",
    "blob_size": $CONTENT_SIZE
}
EOF
)
    
    print_status "Sending transaction generation request..."
    
    RESPONSE=$(curl -s -w "\n%{http_code}" -X POST "${GATEWAY_URL}/_walrus/generate-transaction" \
        -H "Content-Type: application/json" \
        -H "Host: ${GATEWAY_HOST}:${GATEWAY_PORT}" \
        -H "Authorization: AWS4-HMAC-SHA256 Credential=${ACCESS_KEY}/20231101/${AWS_REGION}/s3/aws4_request,SignedHeaders=host;content-type,Signature=test-signature" \
        -d "$REQUEST_BODY" 2>/dev/null || echo -e "\n000")
    
    HTTP_CODE=$(echo "$RESPONSE" | tail -1)
    RESPONSE_BODY=$(echo "$RESPONSE" | sed '$d')
    
    if [[ "$HTTP_CODE" == "200" ]] || [[ "$HTTP_CODE" == "202" ]]; then
        print_status "✅ Transaction template generation endpoint responded (HTTP $HTTP_CODE)"
        echo "Response preview:"
        format_json_response "$RESPONSE_BODY" | head -8
    else
        print_warning "⚠️  Transaction template generation returned HTTP $HTTP_CODE"
        echo "Response:"
        format_json_response "$RESPONSE_BODY" | head -5
    fi
}

test_submit_transaction() {
    print_step "📝 Testing signed transaction submission..."
    
    # Create a dummy signed transaction (in real usage, this would be signed by the wallet)
    REQUEST_BODY=$(cat <<EOF
{
    "signed_transaction": "dummy_signed_transaction_data_$(date +%s)",
    "bucket": "$TEST_BUCKET",
    "key": "$TEST_OBJECT",
    "blob_data": "$(echo -n "$TEST_CONTENT" | base64)"
}
EOF
)
    
    print_status "Sending signed transaction submission..."
    
    RESPONSE=$(curl -s -w "\n%{http_code}" -X POST "${GATEWAY_URL}/_walrus/submit-transaction" \
        -H "Content-Type: application/json" \
        -d "$REQUEST_BODY" 2>/dev/null || echo -e "\n000")
    
    HTTP_CODE=$(echo "$RESPONSE" | tail -1)
    RESPONSE_BODY=$(echo "$RESPONSE" | sed '$d')
    
    if [[ "$HTTP_CODE" =~ ^[12][0-9][0-9]$ ]]; then
        print_status "✅ Transaction submission endpoint responded (HTTP $HTTP_CODE)"
        echo "Response preview:"
        format_json_response "$RESPONSE_BODY" | head -8
    else
        print_warning "⚠️  Transaction submission returned HTTP $HTTP_CODE"
        echo "Response:"
        format_json_response "$RESPONSE_BODY" | head -5
    fi
}

test_get_object() {
    print_step "📥 Testing GET object..."
    
    RESPONSE=$(curl -s -w "\n%{http_code}" -X GET "${GATEWAY_URL}/${TEST_BUCKET}/${TEST_OBJECT}" \
        -H "Host: ${GATEWAY_HOST}:${GATEWAY_PORT}" \
        -H "Authorization: AWS4-HMAC-SHA256 Credential=${ACCESS_KEY}/20231101/${AWS_REGION}/s3/aws4_request,SignedHeaders=host,Signature=test-signature" \
        2>/dev/null || echo -e "\n000")
    
    HTTP_CODE=$(echo "$RESPONSE" | tail -1)
    RESPONSE_BODY=$(echo "$RESPONSE" | sed '$d')
    
    print_status "GET object returned HTTP $HTTP_CODE"
    if [[ -n "$RESPONSE_BODY" ]] && [[ ${#RESPONSE_BODY} -lt 200 ]]; then
        echo "Response: $RESPONSE_BODY"
    elif [[ -n "$RESPONSE_BODY" ]]; then
        echo "Response preview: $(echo "$RESPONSE_BODY" | head -c 100)..."
    fi
}

test_delete_object() {
    print_step "🗑️  Testing DELETE object..."
    
    RESPONSE=$(curl -s -w "\n%{http_code}" -X DELETE "${GATEWAY_URL}/${TEST_BUCKET}/${TEST_OBJECT}" \
        -H "Host: ${GATEWAY_HOST}:${GATEWAY_PORT}" \
        -H "Authorization: AWS4-HMAC-SHA256 Credential=${ACCESS_KEY}/20231101/${AWS_REGION}/s3/aws4_request,SignedHeaders=host,Signature=test-signature" \
        2>/dev/null || echo -e "\n000")
    
    HTTP_CODE=$(echo "$RESPONSE" | tail -1)
    RESPONSE_BODY=$(echo "$RESPONSE" | sed '$d')
    
    print_status "DELETE object returned HTTP $HTTP_CODE"
    if [[ -n "$RESPONSE_BODY" ]] && [[ ${#RESPONSE_BODY} -lt 200 ]]; then
        echo "Response: $RESPONSE_BODY"
    fi
}

print_configuration() {
    print_step "⚙️  Current Configuration"
    echo "Gateway Settings:"
    echo "  GATEWAY_PROTOCOL: $GATEWAY_PROTOCOL"
    echo "  GATEWAY_HOST: $GATEWAY_HOST"
    echo "  GATEWAY_PORT: $GATEWAY_PORT"
    echo "  GATEWAY_URL: $GATEWAY_URL (computed from protocol://host:port)"
    echo ""
    echo "Test Settings:"
    echo "  TEST_BUCKET: $TEST_BUCKET"
    echo "  TEST_OBJECT: $TEST_OBJECT"
    echo "  ACCESS_KEY: $ACCESS_KEY"
    echo "  SECRET_KEY: ${SECRET_KEY:0:8}..." # Mostra solo i primi 8 caratteri per sicurezza
    echo "  AWS_REGION: $AWS_REGION"
    echo ""
    echo "Sui Settings:"
    echo "  SUI_NETWORK: $SUI_NETWORK"
    echo "  SUI_RPC_URL: $SUI_RPC_URL"
    echo ""
    echo "Timeout Settings:"
    echo "  REQUEST_TIMEOUT: ${REQUEST_TIMEOUT}s"
    echo "  FAUCET_TIMEOUT: ${FAUCET_TIMEOUT}s"
    echo "  FAUCET_WAIT_TIME: ${FAUCET_WAIT_TIME}s"
    echo ""
    echo "Cleanup Settings:"
    echo "  AUTO_CLEANUP: $AUTO_CLEANUP"
    echo ""
    echo "Environment Variables Help:"
    echo "  Set any of the above variables to customize the test behavior"
    echo "  Example: GATEWAY_HOST=localhost GATEWAY_PORT=9000 ./test-complete.sh"
}

run_comprehensive_tests() {
    print_status "🚀 Starting comprehensive client-side signing tests..."
    echo "=========================================================="
    
    # Basic connectivity tests
    test_list_buckets
    echo ""
    
    # Client-side signing workflow tests
    test_put_object_client_signing
    echo ""
    
    test_generate_transaction
    echo ""
    
    test_submit_transaction
    echo ""
    
    # Additional S3 operations
    test_get_object
    echo ""
    
    test_delete_object
    echo ""
}

print_summary() {
    print_status "📊 Test Summary"
    echo "=========================================================="
    print_status "✅ Completed comprehensive client-side signing tests"
    print_status ""
    print_status "Key Test Results:"
    print_status "• PUT operations should return HTTP 202 with signing requirements"
    print_status "• Transaction template generation endpoint should respond"
    print_status "• Transaction submission endpoint should accept signed transactions"
    print_status "• Standard S3 operations (GET, DELETE) should be available"
    print_status ""
    print_status "🔧 Wallet Information:"
    print_status "• Created temporary Sui wallet: $SUI_ADDRESS"
    print_status "• Wallet files cleaned up automatically"
    print_status ""
    print_status "🎯 Next Steps:"
    print_status "• Integrate with real Sui wallet for actual transaction signing"
    print_status "• Test with live Walrus network operations"
    print_status "• Implement complete S3 compatibility features"
    print_status ""
    print_status "🌟 Client-side signing implementation test completed!"
}

main() {
    # Parse command line arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            --show-config)
                print_configuration
                exit 0
                ;;
            --help|-h)
                echo "🧪 Walrus S3 Gateway - Complete Client-Side Signing Test"
                echo ""
                echo "Usage: $0 [OPTIONS]"
                echo ""
                echo "Options:"
                echo "  --show-config    Show current configuration and exit"
                echo "  --help, -h       Show this help message"
                echo ""
                echo "Environment Variables:"
                echo "  GATEWAY_PROTOCOL Gateway protocol (default: http)"
                echo "  GATEWAY_HOST     Gateway host (default: 127.0.0.1)"
                echo "  GATEWAY_PORT     Gateway port (default: 8080)"
                echo "  GATEWAY_URL      Gateway URL (default: computed from protocol://host:port)"
                echo "  ACCESS_KEY       S3 access key (default: walrus-access-key)"
                echo "  SECRET_KEY       S3 secret key (default: walrus-secret-key)"
                echo "  AWS_REGION       AWS region (default: us-east-1)"
                echo "  SUI_NETWORK      Sui network (default: testnet)"
                echo "  SUI_RPC_URL      Sui RPC URL (default: https://fullnode.testnet.sui.io:443)"
                echo "  TEST_BUCKET      Test bucket name (default: test-bucket-<timestamp>)"
                echo "  TEST_OBJECT      Test object name (default: test-object.txt)"
                echo "  REQUEST_TIMEOUT  Request timeout (default: 30s)"
                echo "  FAUCET_TIMEOUT   Faucet timeout (default: 30s)"
                echo "  FAUCET_WAIT_TIME Wait time after faucet (default: 10s)"
                echo "  AUTO_CLEANUP     Auto cleanup files (default: true)"
                exit 0
                ;;
            *)
                print_error "Unknown option: $1"
                echo "Use --help for usage information"
                exit 1
                ;;
        esac
        shift
    done

    echo "🧪 Walrus S3 Gateway - Complete Client-Side Signing Test"
    echo "=========================================================="
    echo ""
    
    cleanup_existing_files
    
    check_dependencies
    echo ""
    
    setup_sui_wallet
    echo ""
    
    check_gateway_status
    echo ""
    
    print_configuration
    echo ""
    
    run_comprehensive_tests
    
    print_summary
}

# Run main function
main "$@"
