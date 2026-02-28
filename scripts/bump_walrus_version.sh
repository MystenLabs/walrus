#!/usr/bin/env bash
# Copyright (c) Walrus Foundation
# SPDX-License-Identifier: Apache-2.0
#
# DEPRECATED: This script has been replaced by .github/workflows/version-bump.yml
#
# Use the reusable workflow instead:
#   gh workflow run version-bump.yml -f type=minor -f delivery=pr
#   gh workflow run version-bump.yml -f type=patch -f delivery=direct
#
# Or trigger via gen-version-bump-pr.yml (which calls version-bump.yml internally).

echo "ERROR: This script is deprecated." >&2
echo "Use the version-bump.yml workflow instead:" >&2
echo "  gh workflow run version-bump.yml -f type=minor -f delivery=pr" >&2
exit 1
