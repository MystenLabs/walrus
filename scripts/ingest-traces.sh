#!/usr/bin/env bash
# Copyright (c) Walrus Foundation
# SPDX-License-Identifier: Apache-2.0

#
# Ingests a trace from a GZIP file in the OTLP File Exporter format, such as
# is exported by the `--trace-cli` argument of the walrus CLI tool.
#
# See https://opentelemetry.io/docs/specs/otel/protocol/file-exporter/ for the
# format.
set -eou pipefail

readonly FILE="${1:?The first argument must be the gzipped file}"
readonly URL="http://localhost:4318/v1/traces"

gzip -cd "${FILE}" | while IFS= read -r line; do
  # skip empty lines
  [ -z "$line" ] && continue

  printf '%s' "$line" \
    | curl -f -X POST -H 'Content-Type: application/json' --compressed -d @- "${URL}"
done
