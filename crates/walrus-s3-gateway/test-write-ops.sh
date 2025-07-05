#!/bin/bash

# Test script for Walrus S3 Gateway with write operations
# Prerequisites:
# 1. Sui CLI installed and configured
# 2. User wallet set up with testnet tokens
# 3. Gateway running on localhost:8080

echo "Testing Walrus S3 Gateway Write Operations"
echo "========================================="

# Configuration
GATEWAY_URL="http://localhost:8080"
ACCESS_KEY="test-user"
SECRET_KEY="test-secret-123"
BUCKET="test-bucket"
TEST_FILE="test-file.txt"

# Create a test file
echo "Hello from Walrus S3 Gateway!" > $TEST_FILE
echo "Created test file: $TEST_FILE"

# Configure AWS CLI
aws configure set aws_access_key_id $ACCESS_KEY
aws configure set aws_secret_access_key $SECRET_KEY
aws configure set region us-east-1
echo "Configured AWS CLI with test credentials"

# Test 1: List buckets
echo ""
echo "Test 1: List buckets"
aws --endpoint-url $GATEWAY_URL s3 ls

# Test 2: Create bucket (should work even though Walrus doesn't have buckets)
echo ""
echo "Test 2: Create bucket"
aws --endpoint-url $GATEWAY_URL s3 mb s3://$BUCKET

# Test 3: Upload file (PUT operation)
echo ""
echo "Test 3: Upload file"
aws --endpoint-url $GATEWAY_URL s3 cp $TEST_FILE s3://$BUCKET/$TEST_FILE

# Test 4: List objects in bucket
echo ""
echo "Test 4: List objects in bucket"
aws --endpoint-url $GATEWAY_URL s3 ls s3://$BUCKET/

# Test 5: Download file (GET operation)
echo ""
echo "Test 5: Download file"
aws --endpoint-url $GATEWAY_URL s3 cp s3://$BUCKET/$TEST_FILE downloaded-$TEST_FILE

# Test 6: Compare files
echo ""
echo "Test 6: Compare original and downloaded files"
if diff $TEST_FILE downloaded-$TEST_FILE; then
    echo "✅ Files match - upload/download successful!"
else
    echo "❌ Files don't match - something went wrong"
fi

# Test 7: Delete file
echo ""
echo "Test 7: Delete file"
aws --endpoint-url $GATEWAY_URL s3 rm s3://$BUCKET/$TEST_FILE

# Test 8: Try to download deleted file (should fail)
echo ""
echo "Test 8: Try to download deleted file"
if aws --endpoint-url $GATEWAY_URL s3 cp s3://$BUCKET/$TEST_FILE deleted-$TEST_FILE 2>/dev/null; then
    echo "❌ File still accessible after deletion"
else
    echo "✅ File correctly deleted"
fi

# Cleanup
rm -f $TEST_FILE downloaded-$TEST_FILE deleted-$TEST_FILE

echo ""
echo "Test completed!"
echo ""
echo "Note: Make sure you have:"
echo "1. Sui wallet configured with testnet tokens"
echo "2. Keystore copied to ~/.sui/sui_config/sui.keystore.$ACCESS_KEY"
echo "3. Gateway running with the new per-request client architecture"
