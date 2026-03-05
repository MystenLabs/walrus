#!/bin/bash
# Copyright (c) Walrus Foundation
# SPDX-License-Identifier: Apache-2.0
#
# Ping test to verify a site is live after deployment.
# Usage: ./ping-test.sh <url> [--retries <n>] [--delay <seconds>] [--timeout <seconds>]
#   url       The URL to check (required)
#   --retries Number of attempts before failing (default: 10)
#   --delay   Seconds to wait between retries (default: 15)
#   --timeout Seconds before a single request times out (default: 10)
#
# Example:
#   ./ping-test.sh https://docs.wal.app --retries 12 --delay 20

set -euo pipefail

# ── Parse args ────────────────────────────────────────────────────────────────
URL=""
RETRIES=10
DELAY=15
TIMEOUT=10

while [[ $# -gt 0 ]]; do
  case "$1" in
    --retries) RETRIES="$2"; shift 2 ;;
    --delay)   DELAY="$2";   shift 2 ;;
    --timeout) TIMEOUT="$2"; shift 2 ;;
    -*)        echo "Unknown option: $1"; exit 1 ;;
    *)         URL="$1"; shift ;;
  esac
done

if [[ -z "$URL" ]]; then
  echo "Error: URL is required"
  echo "Usage: $0 <url> [--retries <n>] [--delay <seconds>] [--timeout <seconds>]"
  exit 1
fi

# ── Ping loop ─────────────────────────────────────────────────────────────────
echo "Pinging $URL"
echo "  retries : $RETRIES"
echo "  delay   : ${DELAY}s"
echo "  timeout : ${TIMEOUT}s"
echo "================================================"

attempt=1
while [[ $attempt -le $RETRIES ]]; do
  echo "Attempt $attempt / $RETRIES..."

  HTTP_STATUS=$(curl \
    --silent \
    --output /dev/null \
    --write-out "%{http_code}" \
    --max-time "$TIMEOUT" \
    --location \
    "$URL" || echo "000")

  if [[ "$HTTP_STATUS" =~ ^2 ]]; then
    echo "================================================"
    echo "✅ Site is live! ($URL returned HTTP $HTTP_STATUS)"
    exit 0
  else
    echo "   Got HTTP $HTTP_STATUS — site not ready yet"
    if [[ $attempt -lt $RETRIES ]]; then
      echo "   Waiting ${DELAY}s before next attempt..."
      sleep "$DELAY"
    fi
  fi

  ((attempt++))
done

echo "================================================"
echo "❌ Site did not respond with a 2xx status after $RETRIES attempts ($URL)"
exit 1