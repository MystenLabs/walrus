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

# By default run all walrus simtests (filtered by the `walrus-simtest` binary name), if not
# specified. Set TEST_FILTER to a specific test name to run only that test, e.g.
# TEST_FILTER=test_repeated_node_crash.
: ${TEST_FILTER:=simtest}

echo ""
echo "================================================"
echo "Running e2e simtests with $TEST_NUM iterations"
echo "Test filter: $TEST_FILTER"
echo "================================================"
date

if [ "$TEST_FILTER" != "simtest" ] && [ "$TEST_NUM" -gt 1 ]; then
  # A specific test is selected and multiple iterations are requested. MSIM_TEST_NUM runs the
  # iterations sequentially inside a single test execution, so a single test would only use one
  # CPU. Instead, run TEST_NUM jobs in parallel, each running the test once with a distinct seed.

  # Build the test binaries once up front so that the parallel jobs don't race to compile.
  TMPDIR="$WALRUS_TMP_DIR" \
  scripts/simtest/cargo-simtest simtest build --tests 2>&1 | tee "$LOG_FILE"

  # Each simtest iteration is single-threaded, so cap the number of concurrent jobs at NUM_CPUS.
  MAX_PARALLEL_JOBS=${MAX_PARALLEL_JOBS:-$NUM_CPUS}
  [ "$MAX_PARALLEL_JOBS" -lt 1 ] && MAX_PARALLEL_JOBS=1

  for ((ITER = 0; ITER < TEST_NUM; ITER++)); do
    # Wait for a free job slot before starting the next iteration.
    while [ "$(jobs -rp | wc -l)" -ge "$MAX_PARALLEL_JOBS" ]; do
      sleep 5
    done

    ITER_SEED=$((SEED + ITER))
    ITER_LOG_FILE="$LOG_DIR/log-iteration-$ITER"
    echo "Starting iteration $ITER with seed $ITER_SEED (log: $ITER_LOG_FILE)"

    TMPDIR="$WALRUS_TMP_DIR" \
    MSIM_TEST_SEED="$ITER_SEED" \
    MSIM_WATCHDOG_TIMEOUT_MS="$WATCHDOG_TIMEOUT_MS" \
    scripts/simtest/cargo-simtest simtest "$TEST_FILTER" \
      --color never \
      --test-threads 1 \
      --profile simtestnightly > "$ITER_LOG_FILE" 2>&1 &
  done
else
  # This command runs many different tests, so each iteration already uses all CPUs fairly
  # efficiently. Run the iterations sequentially rather than in parallel, each with a distinct seed.

  for ((ITER = 0; ITER < TEST_NUM; ITER++)); do
    ITER_SEED=$((SEED + ITER))
    echo "Starting iteration $ITER with seed $ITER_SEED"

    TMPDIR="$WALRUS_TMP_DIR" \
    MSIM_TEST_SEED="$ITER_SEED" \
    MSIM_WATCHDOG_TIMEOUT_MS="$WATCHDOG_TIMEOUT_MS" \
    scripts/simtest/cargo-simtest simtest "$TEST_FILTER" \
      --color never \
      --test-threads "$NUM_CPUS" \
      --profile simtestnightly 2>&1 | tee -a "$LOG_FILE"
  done
fi

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
