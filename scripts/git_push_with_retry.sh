#!/usr/bin/env bash
# Copyright (c) Walrus Foundation
# SPDX-License-Identifier: Apache-2.0
#
# Runs `git push` with the given arguments, retrying to absorb transient network failures.
#
# Every retryable failure is retried, not just network ones: distinguishing them would mean
# pattern-matching git's stderr, which is brittle. A genuinely rejected push (non-fast-forward,
# for example) fails all attempts and still exits non-zero, just 15 seconds later.
#
# Usage: git_push_with_retry.sh <git push arguments...>

set -Eeuo pipefail

if [[ $# -eq 0 ]]; then
  echo "USAGE: git_push_with_retry.sh <git push arguments...>" >&2
  exit 1
fi

ATTEMPTS="${GIT_PUSH_ATTEMPTS:-3}"

for ((attempt = 1; attempt <= ATTEMPTS; attempt++)); do
  if git push "$@"; then
    exit 0
  fi
  echo "Warning: push attempt ${attempt}/${ATTEMPTS} failed" >&2
  if ((attempt < ATTEMPTS)); then
    sleep $((attempt * 5))
  fi
done

echo "Error: git push failed after ${ATTEMPTS} attempts" >&2
exit 1
