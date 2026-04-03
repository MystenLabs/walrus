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
#
# Epoch bucket design:
#   Storage nodes label consistency-check metrics with `epoch % 1000` (see
#   EPOCH_BUCKET_COUNT in consistency_check.rs). This bounds Prometheus label
#   cardinality while ensuring buckets are never reused within any realistic
#   test duration. The observer compares all common epoch labels across nodes.
#   If any node has more epoch labels than MAX_EPOCH_BUCKETS, we stop comparing
#   and log a warning, because bucket reuse could cause false positives — a
#   stale value from epoch N in the same bucket as epoch N+1000 looks like a
#   data divergence even though the node simply hasn't updated yet.

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
# Rounds before we escalate "no event source data" from info to warning.
# Event source metric requires 20k events (~1.5h). With 60s intervals,
# 120 rounds ≈ 2 hours — enough time for the metric to appear.
EVENT_SOURCE_WARN_AFTER="${EVENT_SOURCE_WARN_AFTER:-120}"
# Must match EPOCH_BUCKET_COUNT in consistency_check.rs. When a node has this
# many epoch labels, buckets will start being reused and comparisons become
# unreliable.
MAX_EPOCH_BUCKETS="${MAX_EPOCH_BUCKETS:-1000}"

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
        log "  node-${i} (${NODES[$i]}):${vals:- (no data — not enough events processed yet)}"
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
#
# Example input line:
#   walrus_blob_info_consistency_check{epoch="5"} 3965707792766453120
# With metric="walrus_blob_info_consistency_check" label="epoch", outputs:
#   5	3965707792766453120
extract_metric() {
    local file="$1" metric="$2" label="$3"
    # 1. grep: find lines starting with the metric name
    # 2. sed:  extract the label value and numeric value
    # 3. sort: order by label value for consistent comparison
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
# Arguments: metric_name  label_name  details_file  [max_labels]
# Echoes:
#   -2  if any node has >= max_labels labels (bucket saturation)
#   -1  if no common labels exist
#    0  if all common labels match
#   >0  number of mismatched label values
# ---------------------------------------------------------------------------
check_cross_node_metric() {
    local metric="$1" label="$2" details_file="$3"
    local max_labels="${4:-0}"
    local num_nodes=${#NODES[@]}
    local violations=0

    : > "$details_file"

    # Step 1: Extract label→value pairs from each node's scrape into TSV files.
    #   chk_0.tsv, chk_1.tsv, ... each contain lines like: "5\t3965707792766453120"
    for i in "${!NODES[@]}"; do
        extract_metric "$WORK_DIR/raw_${NODES[$i]}.prom" "$metric" "$label" \
            > "$WORK_DIR/chk_${i}.tsv"
    done

    # Step 2: Bail out if epoch labels are approaching bucket reuse.
    #   Labels are `epoch % EPOCH_BUCKET_COUNT`. If the highest label on any node
    #   is >= 90% of max_labels, we're nearing wraparound where new epochs overwrite
    #   old ones in the same bucket, making cross-node comparison unreliable.
    if [ "$max_labels" -gt 0 ]; then
        local threshold=$((max_labels * 9 / 10))
        for i in "${!NODES[@]}"; do
            local highest
            highest=$(cut -f1 "$WORK_DIR/chk_${i}.tsv" | sort -n | tail -1)
            if [ -n "$highest" ] && [ "$highest" -ge "$threshold" ]; then
                echo "-2"
                return
            fi
        done
    fi

    # Step 3: Find labels present on ALL nodes (intersection).
    #   We only compare a label if every node has reported it. This avoids false
    #   positives when a node is lagging and hasn't computed a recent epoch yet.
    #   Example: if nodes report epochs {1,2,3}, {1,2,3}, {1,2}, {1,2,3},
    #   only epochs 1 and 2 are compared (present on all 4 nodes).
    for i in "${!NODES[@]}"; do
        cut -f1 "$WORK_DIR/chk_${i}.tsv"
    done | sort | uniq -c | awk -v n="$num_nodes" '$1 == n { print $2 }' \
        > "$WORK_DIR/chk_common.txt"

    if [ ! -s "$WORK_DIR/chk_common.txt" ]; then
        echo "-1"  # no common labels — nothing to compare
        return
    fi

    # Step 4: For each common label, verify all nodes report the same value.
    while IFS= read -r lbl; do
        [ -z "$lbl" ] && continue
        local ref_val="" mismatch=false

        # Collect the value for this label from each node; compare to first node's value.
        for i in "${!NODES[@]}"; do
            local val
            val=$(awk -F'\t' -v l="$lbl" '$1 == l { print $2 }' "$WORK_DIR/chk_${i}.tsv")
            if [ -z "$ref_val" ]; then
                ref_val="$val"
            elif [ "$val" != "$ref_val" ]; then
                mismatch=true
            fi
        done

        # Record the mismatch with per-node details for debugging.
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
# Check walrus_node_blob_data_fully_stored_ratio == 1 for each node's latest
# epoch bucket.  With EPOCH_BUCKET_COUNT = 1_000, buckets map 1:1 to real
# epochs for any realistic test duration.  We only check the latest (highest)
# epoch per node because older epochs are immutable snapshots — if the latest
# epoch is healthy, earlier ones are too.
# Echoes the number of violations.
# ---------------------------------------------------------------------------
check_fully_stored_ratio() {
    local metric="walrus_node_blob_data_fully_stored_ratio"
    local details_file="$1"
    local violations=0

    : > "$details_file"

    for i in "${!NODES[@]}"; do
        # Find the highest epoch bucket on this node (numerically sorted).
        # This is the most recent consistency check result. Older epochs are
        # immutable — they can't degrade, so only the latest matters.
        local latest=""
        latest=$(extract_metric "$WORK_DIR/raw_${NODES[$i]}.prom" "$metric" "epoch" \
            | sort -t"$(printf '\t')" -k1,1n | tail -1)
        [ -z "$latest" ] && continue

        # Split "epoch_label\tratio_value" into separate variables.
        local epoch ratio
        epoch=$(echo "$latest" | cut -f1)
        ratio=$(echo "$latest" | cut -f2)

        # Ratio must be exactly 1.0 (all sampled blobs fully stored).
        # Use awk for float comparison since bash can't compare decimals.
        if ! awk -v r="$ratio" 'BEGIN { exit (r == 1) ? 0 : 1 }'; then
            echo "  node-${i} (${NODES[$i]}) epoch=${epoch} ratio=${ratio}" >> "$details_file"
            violations=$((violations + 1))
        fi
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
log "Max epoch buckets: ${MAX_EPOCH_BUCKETS}"

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
    #
    # The epoch label is `epoch % 1_000` (EPOCH_BUCKET_COUNT in
    # consistency_check.rs). With 1000 buckets, reuse cannot happen in any
    # realistic test duration. We pass MAX_EPOCH_BUCKETS so that if a test
    # ever runs long enough to saturate the buckets, we stop comparing
    # rather than risk false positives from stale collisions.
    # ------------------------------------------------------------------
    v=$(check_cross_node_metric \
        "walrus_blob_info_consistency_check" "epoch" \
        "$WORK_DIR/details_blob_info.txt" "$MAX_EPOCH_BUCKETS")
    if [ "$v" -eq -2 ]; then
        log "WARNING: walrus_blob_info_consistency_check: epoch bucket capacity reached" \
            "(${MAX_EPOCH_BUCKETS}), skipping comparison to avoid false positives from bucket reuse"
        print_metric_values "walrus_blob_info_consistency_check" "epoch"
    elif [ "$v" -gt 0 ]; then
        log "INVARIANT VIOLATION — blob_info_consistency_check (${v} epoch(s)):"
        cat "$WORK_DIR/details_blob_info.txt"
        print_metric_values "walrus_blob_info_consistency_check" "epoch"
        die "blob_info_consistency_check: mismatched digests across nodes"
    else
        log "walrus_blob_info_consistency_check: OK"
        print_metric_values "walrus_blob_info_consistency_check" "epoch"
    fi

    # ------------------------------------------------------------------
    # Hard invariant 2: per-object blob digest must match across nodes.
    # Same epoch bucket design as invariant 1; see comment above.
    # ------------------------------------------------------------------
    v=$(check_cross_node_metric \
        "walrus_per_object_blob_info_consistency_check" "epoch" \
        "$WORK_DIR/details_per_object.txt" "$MAX_EPOCH_BUCKETS")
    if [ "$v" -eq -2 ]; then
        log "WARNING: walrus_per_object_blob_info_consistency_check: epoch bucket capacity reached" \
            "(${MAX_EPOCH_BUCKETS}), skipping comparison to avoid false positives from bucket reuse"
        print_metric_values "walrus_per_object_blob_info_consistency_check" "epoch"
    elif [ "$v" -gt 0 ]; then
        log "INVARIANT VIOLATION — per_object_blob_info_consistency_check (${v} epoch(s)):"
        cat "$WORK_DIR/details_per_object.txt"
        print_metric_values "walrus_per_object_blob_info_consistency_check" "epoch"
        die "per_object_blob_info_consistency_check: mismatched digests across nodes"
    else
        log "walrus_per_object_blob_info_consistency_check: OK"
        print_metric_values "walrus_per_object_blob_info_consistency_check" "epoch"
    fi

    # ------------------------------------------------------------------
    # Soft invariant 1: event source consistency (tolerate transient lag).
    # Nodes may be at different event-processing positions, so bucket
    # values can temporarily diverge. Crash only after a persistent
    # mismatch across EVENT_SOURCE_PATIENCE consecutive rounds.
    # ------------------------------------------------------------------
    v=$(check_cross_node_metric \
        "walrus_periodic_event_source_for_deterministic_events" "bucket" \
        "$WORK_DIR/details_event_source.txt")
    if [ "$v" -eq -1 ]; then
        # No data yet — the metric is recorded every 20k events (~1.5h).
        event_source_streak=0
        if [ "$round" -lt "$EVENT_SOURCE_WARN_AFTER" ]; then
            log "walrus_periodic_event_source_for_deterministic_events: no data yet" \
                "(expected — metric requires ~20k events, round ${round}/${EVENT_SOURCE_WARN_AFTER})"
        else
            log "WARNING: walrus_periodic_event_source_for_deterministic_events: still no data" \
                "after ${round} rounds — expected by now, check event processing"
        fi
        print_metric_values "walrus_periodic_event_source_for_deterministic_events" "bucket"
    elif [ "$v" -gt 0 ]; then
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
    # Only the latest (highest) epoch bucket per node is checked — see
    # check_fully_stored_ratio for rationale.
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
