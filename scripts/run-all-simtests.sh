#!/bin/bash
# Copyright (c) Walrus Foundation
# SPDX-License-Identifier: Apache-2.0

# Script to run all simtests with seed-search.py
# Usage: ./scripts/run-all-simtests.sh [--num-seeds NUM] [--seed-start START] [--concurrency CONCURRENCY]

# Set up cleanup function for Ctrl+C
cleanup() {
    echo ""
    echo "========================================="
    echo "INTERRUPTED: Cleaning up..."
    echo "========================================="
    # Kill any remaining test processes
    pkill -f "run-simtest-isolated.py" 2>/dev/null
    pkill -f "seed-search.py" 2>/dev/null
    exit 130
}

# Trap SIGINT (Ctrl+C) and SIGTERM
trap cleanup SIGINT SIGTERM

# Default values
NUM_SEEDS=30
SEED_START=0
CONCURRENCY=5
SKIP_CONFIRMATION=false

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --num-seeds)
            NUM_SEEDS="$2"
            shift 2
            ;;
        --seed-start)
            SEED_START="$2"
            shift 2
            ;;
        --concurrency)
            CONCURRENCY="$2"
            shift 2
            ;;
        --yes|-y)
            SKIP_CONFIRMATION=true
            shift
            ;;
        *)
            echo "Unknown option: $1"
            echo "Usage: $0 [--num-seeds NUM] [--seed-start START] [--concurrency CONCURRENCY] [--yes|-y]"
            echo "  --yes, -y: Skip confirmation prompt"
            exit 1
            ;;
    esac
done

echo "========================================="
echo "WARNING: Running all simtests may take HOURS to complete!"
echo "========================================="
echo ""
echo "Configuration:"
echo "  Number of seeds: $NUM_SEEDS"
echo "  Start seed: $SEED_START"
echo "  Concurrency: $CONCURRENCY"
echo ""

# Count total tests to give an estimate
TEST_COUNT=0
for test_file_path in $TEST_FILES; do
    test_functions=$(grep -B1 "async fn test_" "$test_file_path" | \
                    grep "async fn test_" | \
                    sed 's/.*async fn \(test_[a-zA-Z0-9_]*\).*/\1/')
    if [ -n "$test_functions" ]; then
        TEST_COUNT=$((TEST_COUNT + $(echo "$test_functions" | wc -l)))
    fi
done

echo "Found $TEST_COUNT tests to run."
echo "Each test will run with $NUM_SEEDS seeds."
echo ""

if [ "$SKIP_CONFIRMATION" = false ]; then
    echo "Do you want to continue? (yes/no)"
    read -r CONFIRMATION

    if [ "$CONFIRMATION" != "yes" ] && [ "$CONFIRMATION" != "y" ]; then
        echo "Aborted by user."
        exit 0
    fi
else
    echo "Skipping confirmation (--yes flag provided)"
fi

echo ""

# Find all test files in the simtest directory
TEST_DIR="crates/walrus-simtest/tests"
TEST_FILES=$(find "$TEST_DIR" -name "*.rs" -type f | sort)

if [ -z "$TEST_FILES" ]; then
    echo "Error: No test files found in $TEST_DIR"
    exit 1
fi

# Track results
FAILED_TESTS=()
PASSED_TESTS=()
TOTAL_TESTS=0

# Process each test file
for test_file_path in $TEST_FILES; do
    # Get the test file name without extension and path
    test_file=$(basename "$test_file_path" .rs)

    echo "Discovering tests in: $test_file"

    # Extract test function names using grep
    # Look for functions with #[walrus_simtest] attribute
    test_functions=$(grep -B1 "async fn test_" "$test_file_path" | \
                    grep "async fn test_" | \
                    sed 's/.*async fn \(test_[a-zA-Z0-9_]*\).*/\1/')

    if [ -z "$test_functions" ]; then
        echo "  No tests found in $test_file, skipping..."
        continue
    fi

    # Run each test function
    for test_name in $test_functions; do
        TOTAL_TESTS=$((TOTAL_TESTS + 1))

        echo "========================================="
        echo "Running [$TOTAL_TESTS]: $test_file::$test_name"
        echo "========================================="

        # Run the test with seed-search.py in an isolated process group
        # This prevents seed-search.py's kill_child_processes from terminating this script
        # Capture output to check for success/failure since exit codes don't work
        OUTPUT_FILE=$(mktemp)
        ./scripts/run-simtest-isolated.py \
            --num-seeds "$NUM_SEEDS" \
            --concurrency "$CONCURRENCY" \
            --seed-start "$SEED_START" \
            --test "$test_file" \
            "$test_name --include-ignored" 2>&1 | tee "$OUTPUT_FILE"

        # Check if the test passed by looking for success message
        if ! grep -q "All tests passed successfully!" "$OUTPUT_FILE"; then
            echo "❌ FAILED: $test_file::$test_name"
            FAILED_TESTS+=("$test_file::$test_name")
            rm -f "$OUTPUT_FILE"

            # Stop on first failure
            echo ""
            echo "========================================="
            echo "STOPPING: Test failed"
            echo "========================================="
            echo ""
            echo "Summary:"
            echo "  Passed: ${#PASSED_TESTS[@]} tests"
            echo "  Failed: 1 test"
            echo ""
            echo "Failed test:"
            echo "  - $test_file::$test_name"

            exit 1
        else
            echo "✅ PASSED: $test_file::$test_name"
            PASSED_TESTS+=("$test_file::$test_name")
            rm -f "$OUTPUT_FILE"
        fi

        echo ""
    done
done

# Print final summary
echo "========================================="
echo "ALL TESTS COMPLETED SUCCESSFULLY!"
echo "========================================="
echo ""
echo "Summary:"
echo "  Total tests run: ${#PASSED_TESTS[@]}"
echo "  All tests passed ✅"
