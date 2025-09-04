#!/bin/bash
# Copyright (c) Walrus Foundation
# SPDX-License-Identifier: Apache-2.0


# Test script for Walrus Indexer REST API

echo "🐙 Testing Walrus Indexer REST API"
echo "================================="

# Start the indexer in the background
echo "Starting indexer..."
cargo run --bin walrus-indexer -- --db-path ./test-indexer-db &
INDEXER_PID=$!

# Wait for the server to start
echo "Waiting for server to start..."
sleep 3

# Function to test an endpoint
test_endpoint() {
    local name=$1
    local endpoint=$2
    echo -n "Testing $name... "
    response=$(curl -s -o /dev/null -w "%{http_code}" "http://localhost:8080$endpoint")
    if [ "$response" = "200" ]; then
        echo "✅ OK (200)"
    else
        echo "❌ Failed (HTTP $response)"
    fi
}

# Test health endpoint
test_endpoint "Health Check" "/health"

# Test other endpoints (they might return errors but should be reachable)
test_endpoint "Get Blob" "/get_blob?bucket_id=0x0000000000000000000000000000000000000000000000000000000000000000&primary_key=test"
test_endpoint "List Buckets" "/buckets"

# Get the actual health response
echo ""
echo "Health Check Response:"
curl -s "http://localhost:8080/health" | jq .

# Clean up
echo ""
echo "Stopping indexer..."
kill $INDEXER_PID 2>/dev/null
wait $INDEXER_PID 2>/dev/null

# Clean up test database
rm -rf ./test-indexer-db

echo ""
echo "✅ Test complete!"
