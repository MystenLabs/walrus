#!/usr/bin/env sh
# Copyright (c) Walrus Foundation
# SPDX-License-Identifier: Apache-2.0

# Usage:
#   k6-run-multiple.sh <SCRIPT> <PLAN> [<PLAN>...]
#
# Environment variables:
#   TEST_RUN_ID_SUFFIX - If specified, the TEST_RUN_ID environment variable will be set
#     to "${TEST_ID}:${TEST_RUN_ID_SUFFIX}".
#   CONTINUE_ON_ERROR - Either true or false (default). If set to true, do not exit on a test failure.
#   K6_EXPORT_PROMETHEUS - Either true or false. If set to true, each k6 command is run with --out experimental-prometheus-rw
#   K6_REPORT_DIRECTORY - If specified, reports are written to the identified directory.
set -eou pipefail

readonly script="${1:?The first argument must be the script}"
shift

k6_run_plan() {
  plan="$1"
  test_id="${plan}"

  set -- --quiet run --env PLAN="${plan}" --env TEST_ID="${test_id}"
  [ -n "${TEST_RUN_ID_SUFFIX-}" ] && set -- "$@" --env TEST_RUN_ID="${test_id}:${TEST_RUN_ID_SUFFIX}"
  # shellcheck disable=2086 # This is intentional splitting to add to the arguments
  [ "${K6_EXPORT_PROMETHEUS-false}" = "true" ] && set -- "$@" --out experimental-prometheus-rw

  if [ -n "${K6_REPORT_DIRECTORY-}" ]; then
    filename=$(echo "${test_id}" | sed 's/:/-/g')
    export K6_WEB_DASHBOARD=true
    export K6_WEB_DASHBOARD_EXPORT="${K6_REPORT_DIRECTORY}/${filename}.html"
  fi

  echo "> Executing command: k6" "$@" "${script}"
  echo ""
  k6 "$@" "$script"
}

print_summary() {
  echo "=== [ SUMMARY ] ============================================"
  printf "%u tests run, %u passed, %u failed\n" $(( passed + failed )) "${passed}" "${failed}"
}



failed=0
passed=0
plan_num=1
for plan in "$@"; do
  printf "=== [ TEST %u: %s ] ==============================\n" ${plan_num} "${plan}"

  set +e
  k6_run_plan "${plan}"
  status=$?
  set -e
  echo ""

  if [ "${status}" -eq 0 ]; then
    passed=$(( passed + 1 ))
  else
    failed=$(( failed + 1 ))
    if [ "${CONTINUE_ON_ERROR-false}" = "false" ]; then
      print_summary
      exit $status
    fi
  fi

  plan_num=$(( plan_num + 1 ))
done

print_summary
