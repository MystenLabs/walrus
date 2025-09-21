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
#   K6_EXTRA_ARGS - Extra arguments to pass to each k6 invocation.
#   K6_REPORT_DIRECTORY - If specified, reports are written to the identified directory.
set -eou pipefail

readonly script="${1:?The first argument must be the script}"
shift

k6_run_plan() {
  plan="$1"
  test_id="${plan}"

  set -- --quiet run --env PLAN="${plan}" --env TEST_ID="${test_id}"
  [ -n "${TEST_RUN_ID_SUFFIX-}" ] && set -- "$@" --env TEST_RUN_ID="${test_id}:${TEST_RUN_ID_SUFFIX}"
  [ -n "${K6_EXTRA_ARGS-}" ] && set -- "$@" "${K6_EXTRA_ARGS}"

  if [ -n "${K6_REPORT_DIRECTORY-}" ]; then
    filename=$(echo "${test_id}" | sed 's/:/-/g')
    export K6_WEB_DASHBOARD=true
    export K6_WEB_DASHBOARD_EXPORT="${K6_REPORT_DIRECTORY}/${filename}.html"
  fi

  echo "> Executing command: k6" "$@" "${script}"
  echo ""
  k6 "$@" "$script"
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
      break;
    fi
  fi

  plan_num=$(( plan_num + 1 ))
done

echo "=== [ SUMMARY ] ============================================"
printf "%u tests run, %u passed, %u failed\n" $(( passed + failed )) "${passed}" "${failed}"
