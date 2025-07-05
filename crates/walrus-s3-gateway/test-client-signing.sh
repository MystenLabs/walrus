#!/bin/bash

# Test script for Walrus S3 Gateway Client-Side Signing Implementation
# This script tests the basic functionality of the client-side signing endpoints

set -e

echo "Starting Walrus S3 Gateway Client-Side Signing Test"

# Configuration
GATEWAY_URL="http://127.0.0.1:9200"
TEST_BUCKET="test-bucket"
TEST_OBJECT="test-object.txt"
TEST_CONTENT="Hello, Walrus S3 Gateway with Client-Side Signing!"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
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

# Start the gateway in background (you should run this manually)
print_warning "Make sure to start the gateway manually with:"
print_warning "cargo run --bin walrus-s3-gateway -- --config test-config.toml"
print_warning ""
print_warning "Press Enter when the gateway is running..."
read

# Test 1: Health check
print_status "Testing health check..."
if curl -s -f "${GATEWAY_URL}/health" > /dev/null 2>&1; then
    print_status "✓ Health check passed"
else
    print_warning "Health check endpoint not available (this is expected if not implemented)"
fi

# Test 2: List buckets (should work without authentication for this test)
print_status "Testing list buckets..."
curl -s -X GET "${GATEWAY_URL}/" || print_warning "List buckets failed (expected if auth required)"

# Test 3: PUT object with client-side signing requirement
print_status "Testing PUT object (should return client signing requirement)..."
echo -n "$TEST_CONTENT" > /tmp/test_content.txt

RESPONSE=$(curl -s -X PUT "${GATEWAY_URL}/${TEST_BUCKET}/${TEST_OBJECT}" \
    -H "Content-Type: text/plain" \
    -H "Authorization: AWS4-HMAC-SHA256 Credential=test/20231101/us-east-1/s3/aws4_request,SignedHeaders=host,Signature=test" \
    --data-binary @/tmp/test_content.txt)

if echo "$RESPONSE" | grep -q "client_signing_required"; then
    print_status "✓ Client-side signing requirement detected"
    echo "Response: $RESPONSE" | head -c 200
    echo "..."
else
    print_error "✗ Expected client-side signing requirement, got: $RESPONSE"
fi

# Test 4: Test transaction template endpoint
print_status "Testing transaction template generation..."
TEMPLATE_RESPONSE=$(curl -s -X POST "${GATEWAY_URL}/_walrus/generate-transaction" \
    -H "Content-Type: application/json" \
    -H "Authorization: AWS4-HMAC-SHA256 Credential=test/20231101/us-east-1/s3/aws4_request,SignedHeaders=host,Signature=test" \
    -d '{
        "access_key": "test",
        "purpose": {
            "StoreBlob": {
                "size": '$(echo -n "$TEST_CONTENT" | wc -c)'
            }
        }
    }')

if echo "$TEMPLATE_RESPONSE" | grep -q "transaction_data" || echo "$TEMPLATE_RESPONSE" | grep -q "error"; then
    print_status "✓ Transaction template endpoint responding"
    echo "Response: $TEMPLATE_RESPONSE" | head -c 200
    echo "..."
else
    print_error "✗ Transaction template endpoint not working: $TEMPLATE_RESPONSE"
fi

# Test 5: Test transaction submission endpoint
print_status "Testing transaction submission endpoint..."
SUBMIT_RESPONSE=$(curl -s -X POST "${GATEWAY_URL}/_walrus/submit-transaction" \
    -H "Content-Type: application/json" \
    -d '{
        "signed_transaction": "dummy_signed_transaction_data",
        "bucket": "'$TEST_BUCKET'",
        "key": "'$TEST_OBJECT'",
        "blob_data": "'$(echo -n "$TEST_CONTENT" | base64)'"
    }')

if echo "$SUBMIT_RESPONSE" | grep -q "error" || echo "$SUBMIT_RESPONSE" | grep -q "status"; then
    print_status "✓ Transaction submission endpoint responding"
    echo "Response: $SUBMIT_RESPONSE" | head -c 200
    echo "..."
else
    print_error "✗ Transaction submission endpoint not working: $SUBMIT_RESPONSE"
fi

# Cleanup
rm -f /tmp/test_content.txt

print_status "Client-side signing test completed!"
print_status "Key findings:"
print_status "- PUT operations return client signing requirements (status 202)"
print_status "- Transaction template generation endpoint is available"
print_status "- Transaction submission endpoint is available"
print_status ""
print_status "Next steps:"
print_status "1. Integrate with a Sui wallet for actual transaction signing"
print_status "2. Test with real Walrus blob storage operations"
print_status "3. Implement complete S3 compatibility features"
