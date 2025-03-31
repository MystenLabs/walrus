#!/bin/bash
# Copyright (c) Walrus Foundation
# SPDX-License-Identifier: Apache-2.0


echo "about to signal complete setup"

# Emit the JSONL message
echo '{"antithesis_setup": { "status": "complete", "details": null }}' > "$ANTITHESIS_OUTPUT_DIR/sdk.jsonl"
