#!/bin/bash
# Copyright (c) Walrus Foundation
# SPDX-License-Identifier: Apache-2.0

# Cross-node invariant observer for Antithesis testing.
#
# Periodically scrapes Prometheus metrics from all storage nodes and verifies
# cross-node data consistency invariants. Crashes (exits non-zero) when an
# invariant violation is detected, which Antithesis surfaces as a test failure.
#
# Hard invariants (crash on first confirmed violation):
#   - walrus_blob_info_consistency_check           — same digest per epoch across all nodes
#   - walrus_per_object_blob_info_consistency_check — same digest per epoch across all nodes
#
# Soft invariants (crash after persistent violation):
#   - walrus_periodic_event_source_for_deterministic_events — same per bucket across all nodes
#   - walrus_node_blob_data_fully_stored_ratio               — must equal 1 on every node/epoch

set -euo pipefail

# ---------------------------------------------------------------------------
# Configuration (override via environment variables)
# ---------------------------------------------------------------------------
NODES=("10.0.0.10" "10.0.0.11" "10.0.0.12" "10.0.0.13")
METRICS_PORT="${METRICS_PORT:-9184}"
CHECK_INTERVAL="${CHECK_INTERVAL:-60}"
INITIAL_WAIT="${INITIAL_WAIT:-180}"
# Consecutive rounds a soft-invariant violation must persist before crashing.
EVENT_SOURCE_PATIENCE="${EVENT_SOURCE_PATIENCE:-3}"
FULLY_STORED_PATIENCE="${FULLY_STORED_PATIENCE:-3}"

WORK_DIR="/tmp/observer"
mkdir -p "$WORK_DIR"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
log()  { echo "[$(date -u '+%Y-%m-%dT%H:%M:%SZ')] [observer] $*"; }
die()  { log "FATAL: $*" >&2; exit 1; }

# Print per-node metric values for a labelled gauge.
# Arguments: metric_name  label_name
print_metric_values() {
    local metric="$1" label="$2"
    for i in "${!NODES[@]}"; do
        local vals=""
        while IFS="$(printf '\t')" read -r lbl val; do
            [ -z "$lbl" ] && continue
            vals="${vals} ${label}=${lbl}:${val}"
        done < <(extract_metric "$WORK_DIR/raw_${NODES[$i]}.prom" "$metric" "$label")
        log "  node-${i} (${NODES[$i]}):${vals:- (none)}"
    done
}

# Scrape the /metrics endpoint of a storage node.
scrape_node() {
    local ip="$1" out="$2"
    curl -sf --connect-timeout 5 --max-time 10 \
        "http://${ip}:${METRICS_PORT}/metrics" > "$out" 2>/dev/null
}

# Extract (label_value, metric_value) pairs from a Prometheus scrape file.
# Outputs: label_value<TAB>metric_value  (sorted by label).
extract_metric() {
    local file="$1" metric="$2" label="$3"
    { grep "^${metric}{" "$file" 2>/dev/null || true; } \
        | sed -n "s/^${metric}{.*${label}=\"\([^\"]*\)\".*} \([^ ]*\).*/\1\t\2/p" \
        | sort -t"$(printf '\t')" -k1,1
}

# ---------------------------------------------------------------------------
# Cross-node comparison for a labelled gauge metric.
#
# For every label value present in ALL scraped nodes, assert that the metric
# value is identical.  Writes per-violation details to a file.
#
# Arguments: metric_name  label_name  details_file
# Echoes the number of mismatched label values (0 = clean).
# ---------------------------------------------------------------------------
check_cross_node_metric() {
    local metric="$1" label="$2" details_file="$3"
    local num_nodes=${#NODES[@]}
    local violations=0

    : > "$details_file"

    for i in "${!NODES[@]}"; do
        extract_metric "$WORK_DIR/raw_${NODES[$i]}.prom" "$metric" "$label" \
            > "$WORK_DIR/chk_${i}.tsv"
    done

    # Labels that appear in every node's output.
    for i in "${!NODES[@]}"; do
        cut -f1 "$WORK_DIR/chk_${i}.tsv"
    done | sort | uniq -c | awk -v n="$num_nodes" '$1 == n { print $2 }' \
        > "$WORK_DIR/chk_common.txt"

    if [ ! -s "$WORK_DIR/chk_common.txt" ]; then
        log "  ${metric}: no common ${label} values across nodes, skipping comparison"
        echo "0"
        return
    fi

    while IFS= read -r lbl; do
        [ -z "$lbl" ] && continue
        local ref_val="" mismatch=false

        for i in "${!NODES[@]}"; do
            local val
            val=$(awk -F'\t' -v l="$lbl" '$1 == l { print $2 }' "$WORK_DIR/chk_${i}.tsv")
            if [ -z "$ref_val" ]; then
                ref_val="$val"
            elif [ "$val" != "$ref_val" ]; then
                mismatch=true
            fi
        done

        if [ "$mismatch" = true ]; then
            {
                echo "  ${metric}{${label}=\"${lbl}\"}:"
                for i in "${!NODES[@]}"; do
                    local val
                    val=$(awk -F'\t' -v l="$lbl" '$1 == l { print $2 }' "$WORK_DIR/chk_${i}.tsv")
                    echo "    node-${i} (${NODES[$i]}): ${val}"
                done
            } >> "$details_file"
            violations=$((violations + 1))
        fi
    done < "$WORK_DIR/chk_common.txt"

    echo "$violations"
}

# ---------------------------------------------------------------------------
# Check walrus_node_blob_data_fully_stored_ratio == 1 for all nodes/epochs.
# Echoes the number of violations.
# ---------------------------------------------------------------------------
check_fully_stored_ratio() {
    local metric="walrus_node_blob_data_fully_stored_ratio"
    local details_file="$1"
    local violations=0

    : > "$details_file"

    for i in "${!NODES[@]}"; do
        while IFS="$(printf '\t')" read -r epoch ratio; do
            [ -z "$epoch" ] && continue
            # Numeric comparison: ratio must equal 1.
            if ! awk -v r="$ratio" 'BEGIN { exit (r == 1) ? 0 : 1 }'; then
                echo "  node-${i} (${NODES[$i]}) epoch=${epoch} ratio=${ratio}" >> "$details_file"
                violations=$((violations + 1))
            fi
        done < <(extract_metric "$WORK_DIR/raw_${NODES[$i]}.prom" "$metric" "epoch")
    done

    echo "$violations"
}

# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------
log "Cross-node invariant observer starting"
log "Nodes: ${NODES[*]}, port: ${METRICS_PORT}"
log "Check interval: ${CHECK_INTERVAL}s, initial wait: ${INITIAL_WAIT}s"
log "Event source patience: ${EVENT_SOURCE_PATIENCE} rounds"
log "Fully stored patience: ${FULLY_STORED_PATIENCE} rounds"

log "Waiting ${INITIAL_WAIT}s for cluster stabilization..."
sleep "$INITIAL_WAIT"

event_source_streak=0
fully_stored_streak=0
round=0

while true; do
    round=$((round + 1))
    log "=== Round ${round} ==="

    # ------------------------------------------------------------------
    # Scrape all nodes; skip the round if any node is unreachable.
    # ------------------------------------------------------------------
    all_ok=true
    for i in "${!NODES[@]}"; do
        if scrape_node "${NODES[$i]}" "$WORK_DIR/raw_${NODES[$i]}.prom"; then
            log "Scraped node-${i} (${NODES[$i]})"
        else
            log "Cannot reach node-${i} (${NODES[$i]}), skipping round"
            all_ok=false
            break
        fi
    done
    if [ "$all_ok" = false ]; then
        # Reset streak counters so that non-consecutive violations separated
        # by unreachable rounds do not accumulate toward the patience threshold.
        event_source_streak=0
        fully_stored_streak=0
        sleep "$CHECK_INTERVAL"
        continue
    fi

    # ------------------------------------------------------------------
    # Hard invariant 1: certified blob digest must match across nodes.
    # ------------------------------------------------------------------
    v=$(check_cross_node_metric \
        "walrus_blob_info_consistency_check" "epoch" \
        "$WORK_DIR/details_blob_info.txt")
    if [ "$v" -gt 0 ]; then
        log "INVARIANT VIOLATION — blob_info_consistency_check (${v} epoch(s)):"
        cat "$WORK_DIR/details_blob_info.txt"
        print_metric_values "walrus_blob_info_consistency_check" "epoch"
        die "blob_info_consistency_check: mismatched digests across nodes"
    fi
    log "walrus_blob_info_consistency_check: OK"
    print_metric_values "walrus_blob_info_consistency_check" "epoch"

    # ------------------------------------------------------------------
    # Hard invariant 2: per-object blob digest must match across nodes.
    # ------------------------------------------------------------------
    v=$(check_cross_node_metric \
        "walrus_per_object_blob_info_consistency_check" "epoch" \
        "$WORK_DIR/details_per_object.txt")
    if [ "$v" -gt 0 ]; then
        log "INVARIANT VIOLATION — per_object_blob_info_consistency_check (${v} epoch(s)):"
        cat "$WORK_DIR/details_per_object.txt"
        print_metric_values "walrus_per_object_blob_info_consistency_check" "epoch"
        die "per_object_blob_info_consistency_check: mismatched digests across nodes"
    fi
    log "walrus_per_object_blob_info_consistency_check: OK"
    print_metric_values "walrus_per_object_blob_info_consistency_check" "epoch"

    # ------------------------------------------------------------------
    # Soft invariant 1: event source consistency (tolerate transient lag).
    # Nodes may be at different event-processing positions, so bucket
    # values can temporarily diverge. Crash only after a persistent
    # mismatch across EVENT_SOURCE_PATIENCE consecutive rounds.
    # ------------------------------------------------------------------
    v=$(check_cross_node_metric \
        "walrus_periodic_event_source_for_deterministic_events" "bucket" \
        "$WORK_DIR/details_event_source.txt")
    if [ "$v" -gt 0 ]; then
        event_source_streak=$((event_source_streak + 1))
        log "Event source mismatch (streak: ${event_source_streak}/${EVENT_SOURCE_PATIENCE}):"
        cat "$WORK_DIR/details_event_source.txt"
        print_metric_values "walrus_periodic_event_source_for_deterministic_events" "bucket"
        if [ "$event_source_streak" -ge "$EVENT_SOURCE_PATIENCE" ]; then
            die "periodic_event_source: persistent mismatch for ${EVENT_SOURCE_PATIENCE} consecutive rounds"
        fi
    else
        event_source_streak=0
        log "walrus_periodic_event_source_for_deterministic_events: OK"
        print_metric_values "walrus_periodic_event_source_for_deterministic_events" "bucket"
    fi

    # ------------------------------------------------------------------
    # Soft invariant 2: all blobs fully stored (tolerate recovery lag).
    # A node that just recovered may briefly report < 1 while syncing
    # completes.  Crash only after FULLY_STORED_PATIENCE rounds.
    # ------------------------------------------------------------------
    v=$(check_fully_stored_ratio "$WORK_DIR/details_fully_stored.txt")
    if [ "$v" -gt 0 ]; then
        fully_stored_streak=$((fully_stored_streak + 1))
        log "Fully-stored-ratio violation (streak: ${fully_stored_streak}/${FULLY_STORED_PATIENCE}):"
        cat "$WORK_DIR/details_fully_stored.txt"
        print_metric_values "walrus_node_blob_data_fully_stored_ratio" "epoch"
        if [ "$fully_stored_streak" -ge "$FULLY_STORED_PATIENCE" ]; then
            die "node_blob_data_fully_stored_ratio: persistent violation for ${FULLY_STORED_PATIENCE} consecutive rounds"
        fi
    else
        fully_stored_streak=0
        log "walrus_node_blob_data_fully_stored_ratio: OK"
        print_metric_values "walrus_node_blob_data_fully_stored_ratio" "epoch"
    fi

    log "All checks passed for round ${round}"
    sleep "$CHECK_INTERVAL"
done
