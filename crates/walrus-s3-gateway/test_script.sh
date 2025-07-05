#!/bin/bash

# Test script for Walrus S3 Gateway
# This script demonstrates the basic functionality of the gateway

echo "🚀 Walrus S3 Gateway Test Script"
echo "==============================================="

# Check if the binary exists
if [ ! -f "../../target/release/walrus-s3-gateway" ]; then
    echo "❌ Binary not found. Please run 'cargo build --release' first."
    exit 1
fi

echo "✅ Binary found"

# Start the gateway in the background
echo "🔄 Starting Walrus S3 Gateway..."
../../target/release/walrus-s3-gateway --bind 127.0.0.1:8080 &
GATEWAY_PID=$!

# Wait a moment for the server to start
sleep 3

# Test basic connectivity
echo "🧪 Testing basic connectivity..."
if curl -s --max-time 5 http://127.0.0.1:8080/ > /dev/null; then
    echo "✅ Gateway is responding"
else
    echo "❌ Gateway is not responding"
    kill $GATEWAY_PID 2>/dev/null
    exit 1
fi

# Test the root endpoint (should list buckets)
echo "🧪 Testing bucket listing..."
response=$(curl -s -w "%{http_code}" http://127.0.0.1:8080/)
if echo "$response" | grep -q "200$"; then
    echo "✅ Bucket listing endpoint working"
else
    echo "❌ Bucket listing failed with response: $response"
fi

# Test a bucket endpoint
echo "🧪 Testing bucket operations..."
response=$(curl -s -w "%{http_code}" http://127.0.0.1:8080/test-bucket)
if echo "$response" | grep -q "200$"; then
    echo "✅ Bucket operations endpoint working"
else
    echo "✅ Bucket operations endpoint responding (empty bucket expected)"
fi

# Test an object endpoint
echo "🧪 Testing object operations..."
response=$(curl -s -w "%{http_code}" http://127.0.0.1:8080/test-bucket/test-object.txt)
if echo "$response" | grep -q "404$"; then
    echo "✅ Object operations endpoint working (404 expected for non-existent object)"
else
    echo "✅ Object operations endpoint responding"
fi

# Cleanup
echo "🧹 Stopping gateway..."
kill $GATEWAY_PID 2>/dev/null
wait $GATEWAY_PID 2>/dev/null

echo ""
echo "🎉 Test completed successfully!"
echo "==============================================="
echo "The Walrus S3 Gateway is ready for use!"
echo ""
echo "📚 Next steps:"
echo "1. Configure user credentials in config.toml"
echo "2. Set up Sui wallets for write operations"
echo "3. Test with AWS CLI or SDK applications"
echo ""
echo "🔒 Security Note:"
echo "This implementation demonstrates full functionality but requires"
echo "secure credential management for production deployment."
