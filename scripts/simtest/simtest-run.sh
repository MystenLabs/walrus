#!/bin/bash
# Copyright (c) Walrus Foundation
# SPDX-License-Identifier: Apache-2.0

echo "Running Walrus simtests at commit $(git rev-parse HEAD)"

# Function to handle SIGINT signal (Ctrl+C)
cleanup() {
    echo "Cleaning up child processes..."
    # Kill all child processes in the process group of the current script
    kill -- "-$$"
    exit 1
}

# Set up the signal handler
trap cleanup SIGINT

if [ -z "$NUM_CPUS" ]; then
  NUM_CPUS=$(cat /proc/cpuinfo | grep processor | wc -l) # ubuntu
fi

DATE=$(date '+%Y%m%d_%H%M%S')

# Using a random seed derived from the current time for the simulator tests.
SEED=$(date +%s)

# create logs directory
SIMTEST_LOGS_DIR=~/walrus_simtest_logs
[ ! -d ${SIMTEST_LOGS_DIR} ] && mkdir -p ${SIMTEST_LOGS_DIR}
[ ! -d ${SIMTEST_LOGS_DIR}/${DATE} ] && mkdir -p ${SIMTEST_LOGS_DIR}/${DATE}

LOG_DIR="${SIMTEST_LOGS_DIR}/${DATE}"
LOG_FILE="$LOG_DIR/log"

# Specify the temporary directory for the simulator tests.
# Note that publishing contracts requires that the contracts exist in the same file system as the simulator tests.
# Therefore, we cannot simply use the /tmp directory for the simulator tests.
WALRUS_TMP_DIR=~/walrus_simtest_tmp

# Set the LD_LIBRARY_PATH to include the crt-static library. The query here include all the rustlib
# paths for the current toolchain installed. This is to make sure that when we upgrade rust to a
# new version, the ld library path is updated automatically.
RUST_LIB_PATHS=$(find ~/.rustup/toolchains -type d -path "*/lib/rustlib/x86_64-unknown-linux-gnu/lib" 2>/dev/null)
export LD_LIBRARY_PATH=$(echo "$RUST_LIB_PATHS" | tr '\n' ':')${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}

# Use increased watch dog timeout in simtest.
# Walrus simtest using default setup creates a sui cluster in a single thread, and when initializing
# the cluster 5 seconds wall time of idle activity is common especially when initializing the
# database. This often causes the simtest to be killed by the watchdog.
WATCHDOG_TIMEOUT_MS=30000

# By default run 1 iteration for each test, if not specified.
: ${TEST_NUM:=1}

echo ""
echo "================================================"
echo "Running e2e simtests with $TEST_NUM iterations"
echo "================================================"
date

# This command runs many different tests, so it already uses all CPUs fairly efficiently, and
# don't need to be done inside of the for loop below.
# TODO: this logs directly to stdout since it is not being run in parallel. is that ok?

TMPDIR="$WALRUS_TMP_DIR" \
MSIM_TEST_SEED="$SEED" \
MSIM_TEST_NUM=${TEST_NUM} \
MSIM_WATCHDOG_TIMEOUT_MS="$WATCHDOG_TIMEOUT_MS" \
scripts/simtest/cargo-simtest simtest simtest \
  --color never \
  --test-threads "$NUM_CPUS" \
  --profile simtestnightly 2>&1 | tee "$LOG_FILE"

# wait for all the jobs to end
wait

echo ""
echo "============================================="
echo "All tests completed, checking for failures..."
echo "============================================="
date

grep -EqHn 'TIMEOUT|FAIL|STDERR|SIGABRT|error:|Summary.*[1-9][0-9]* failed' "$LOG_DIR"/*

# if grep found no failures exit now
[ $? -eq 1 ] && echo "No test failures detected" && exit 0

echo "Failures detected, printing logs..."

# read all filenames in $LOG_DIR that contain the string "FAIL" into a bash array
# and print the line number and filename for each
readarray -t FAILED_LOG_FILES < <(grep -El 'TIMEOUT|FAIL' "$LOG_DIR"/*)

# iterate over the array and print the contents of each file
for LOG_FILE in "${FAILED_LOG_FILES[@]}"; do
  echo ""
  echo "=============================="
  echo "Failure detected in $LOG_FILE:"
  echo "=============================="
  cat "$LOG_FILE"
done

exit 1
