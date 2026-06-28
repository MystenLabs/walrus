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
#   - walrus_per_object_pooled_blob_info_consistency_check — same digest per epoch across all nodes
#   - walrus_periodic_event_source_for_deterministic_events — same per bucket across all nodes
#
# Soft invariants (crash after persistent violation):
#   - walrus_node_blob_data_fully_stored_ratio               — must equal 1 on every node/epoch
#
# Bucket design (same idea, two independent dimensions):
#   - Per-epoch metrics (blob_info, per_object_blob_info, fully_stored_ratio) are
#     labelled with `epoch % EPOCH_BUCKET_COUNT` (1000, in consistency_check.rs).
#   - The event-source metric is labelled with
#     `(event_index / NUM_EVENTS_PER_DIGEST_RECORDING) % NUM_DIGEST_BUCKETS`
#     (1000, in node.rs) so recent recordings are retained per bucket.
#   Both schemes bound Prometheus label cardinality. The observer compares all
#   common labels across nodes. If any node exceeds MAX_EPOCH_BUCKETS or
#   MAX_DIGEST_BUCKETS, we stop comparing and log a warning, because bucket
#   reuse could cause false positives — a stale value in the same bucket as a
#   newer one would look like a data divergence.

set -euo pipefail

# ---------------------------------------------------------------------------
# Configuration (override via environment variables)
# ---------------------------------------------------------------------------
NODES=("10.0.0.10" "10.0.0.11" "10.0.0.12" "10.0.0.13")
METRICS_PORT="${METRICS_PORT:-9184}"
CHECK_INTERVAL="${CHECK_INTERVAL:-60}"
# Consecutive rounds a soft-invariant violation must persist before crashing.
FULLY_STORED_PATIENCE="${FULLY_STORED_PATIENCE:-3}"
# Consecutive rounds a hard invariant can return "no common data across nodes"
# before we treat it as a silent regression and fail. Without this, a bug that
# stops the consistency-check metrics from being emitted would leave the observer
# reporting success forever. The default is loose enough to tolerate cluster
# warmup and the event-source metric's naturally long recording cadence.
NO_DATA_PATIENCE="${NO_DATA_PATIENCE:-60}"
# Must match EPOCH_BUCKET_COUNT in consistency_check.rs. When a node has this
# many epoch labels, buckets will start being reused and comparisons become
# unreliable.
MAX_EPOCH_BUCKETS="${MAX_EPOCH_BUCKETS:-1000}"
# Must match NUM_DIGEST_BUCKETS in node.rs. When a node has this many event-source
# bucket labels, the bucket counter is about to wrap and old recordings get
# overwritten by new ones, which would look like cross-node divergence.
MAX_DIGEST_BUCKETS="${MAX_DIGEST_BUCKETS:-1000}"

WORK_DIR="/tmp/observer"
mkdir -p "$WORK_DIR"

# Short aliases for long metric names (used in checks and log messages).
BLOB_INFO="walrus_blob_info_consistency_check"
PER_OBJECT_INFO="walrus_per_object_blob_info_consistency_check"
PER_OBJECT_POOLED_INFO="walrus_per_object_pooled_blob_info_consistency_check"
EVENT_SOURCE="walrus_periodic_event_source_for_deterministic_events"
FULLY_STORED_RATIO="walrus_node_blob_data_fully_stored_ratio"

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

# Scrape the /metrics endpoint of a storage node. On failure, curl's stderr
# is captured into <out>.err so the caller can surface it for root-cause of a
# skipped round (connection refused vs. timeout vs. DNS, etc.).
scrape_node() {
    local ip="$1" out="$2"
    curl -sf --connect-timeout 5 --max-time 10 \
        "http://${ip}:${METRICS_PORT}/metrics" > "$out" 2>"${out}.err"
}

# Extract (label_value, metric_value) pairs from a Prometheus scrape file.
# Outputs: label_value<TAB>metric_value  (sorted by label).
#
# Example input line:
#   walrus_blob_info_consistency_check{epoch="5"} 3965707792766453120
# With metric="$BLOB_INFO" label="epoch", outputs:
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
# Echoes (stdout, consumed by $(...) in caller):
#   -2  if labels are nearing bucket saturation
#   -1  if no common labels exist
#    0  if all common labels match
#   >0  number of mismatched label values
#
# Note: status is communicated via stdout echo rather than exit code because
# callers use `v=$(...)` + `[ "$v" -eq N ]` comparisons. Don't add nonzero
# `return` codes here without also updating every caller.
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

    # Step 3: Find labels present on 2+ nodes.
    #   A label is comparable as long as at least two nodes have reported it.
    #   Nodes that haven't computed a given epoch yet are simply excluded from
    #   that label's comparison.
    for i in "${!NODES[@]}"; do
        cut -f1 "$WORK_DIR/chk_${i}.tsv"
    done | sort | uniq -c | awk '$1 >= 2 { print $2 }' \
        > "$WORK_DIR/chk_common.txt"

    if [ ! -s "$WORK_DIR/chk_common.txt" ]; then
        echo "-1"  # no label on 2+ nodes — nothing to compare
        return
    fi

    # Step 4: For each label, verify all nodes that have it report the same value.
    while IFS= read -r lbl; do
        [ -z "$lbl" ] && continue
        local ref_val="" mismatch=false

        for i in "${!NODES[@]}"; do
            local val
            val=$(awk -F'\t' -v l="$lbl" '$1 == l { print $2 }' "$WORK_DIR/chk_${i}.tsv")
            [ -z "$val" ] && continue  # node doesn't have this label, skip
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
                    echo "    node-${i} (${NODES[$i]}): ${val:-(absent)}"
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
#
# Arguments: details_file  [max_labels]
# Echoes:
#   -2  if any node's highest epoch label is nearing bucket wrap
#    0  if all nodes' latest ratio is 1
#   >0  number of nodes whose latest ratio is below 1
# ---------------------------------------------------------------------------
check_fully_stored_ratio() {
    local metric="$FULLY_STORED_RATIO"
    local details_file="$1"
    local max_labels="${2:-0}"
    local violations=0

    : > "$details_file"

    # Bail out if epoch labels are approaching bucket wraparound. After
    # `epoch % EPOCH_BUCKET_COUNT` wraps, `sort -n | tail -1` no longer
    # picks the latest real epoch — a stale value in a high-numbered bucket
    # could be flagged repeatedly and trip the patience counter.
    if [ "$max_labels" -gt 0 ]; then
        local threshold=$((max_labels * 9 / 10))
        for i in "${!NODES[@]}"; do
            local highest
            highest=$(extract_metric "$WORK_DIR/raw_${NODES[$i]}.prom" "$metric" "epoch" \
                | cut -f1 | sort -n | tail -1)
            if [ -n "$highest" ] && [ "$highest" -ge "$threshold" ]; then
                echo "-2"
                return
            fi
        done
    fi

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
log "Check interval: ${CHECK_INTERVAL}s"
log "Fully stored patience: ${FULLY_STORED_PATIENCE} rounds"
log "No-data patience (hard invariants): ${NO_DATA_PATIENCE} rounds"
log "Max epoch buckets: ${MAX_EPOCH_BUCKETS}, max digest buckets: ${MAX_DIGEST_BUCKETS}"

fully_stored_streak=0
blob_info_no_data_streak=0
per_object_no_data_streak=0
per_object_pooled_no_data_streak=0
event_source_no_data_streak=0
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
            err_file="$WORK_DIR/raw_${NODES[$i]}.prom.err"
            err_msg=""
            [ -s "$err_file" ] && err_msg=": $(tr '\n' ' ' <"$err_file" | head -c 200)"
            log "Cannot reach node-${i} (${NODES[$i]}), skipping round${err_msg}"
            all_ok=false
            break
        fi
    done
    if [ "$all_ok" = false ]; then
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
        "$BLOB_INFO" "epoch" \
        "$WORK_DIR/details_blob_info.txt" "$MAX_EPOCH_BUCKETS")
    if [ "$v" -eq -2 ]; then
        log "WARNING: ${BLOB_INFO}: epoch bucket capacity reached" \
            "(${MAX_EPOCH_BUCKETS}), skipping comparison to avoid false positives from bucket reuse"
        print_metric_values "$BLOB_INFO" "epoch"
    elif [ "$v" -eq -1 ]; then
        blob_info_no_data_streak=$((blob_info_no_data_streak + 1))
        log "${BLOB_INFO}: no common data across nodes" \
            "(streak: ${blob_info_no_data_streak}/${NO_DATA_PATIENCE})"
        print_metric_values "$BLOB_INFO" "epoch"
        if [ "$blob_info_no_data_streak" -ge "$NO_DATA_PATIENCE" ]; then
            die "${BLOB_INFO}: no common data for ${NO_DATA_PATIENCE} consecutive rounds — likely silent regression"
        fi
    elif [ "$v" -gt 0 ]; then
        log "INVARIANT VIOLATION — ${BLOB_INFO} (${v} epoch(s)):"
        cat "$WORK_DIR/details_blob_info.txt"
        print_metric_values "$BLOB_INFO" "epoch"
        die "${BLOB_INFO}: mismatched digests across nodes"
    else
        blob_info_no_data_streak=0
        log "${BLOB_INFO}: OK"
        print_metric_values "$BLOB_INFO" "epoch"
    fi

    # ------------------------------------------------------------------
    # Hard invariant 2: per-object blob digest must match across nodes.
    # Same epoch bucket design as invariant 1; see comment above.
    # ------------------------------------------------------------------
    v=$(check_cross_node_metric \
        "$PER_OBJECT_INFO" "epoch" \
        "$WORK_DIR/details_per_object.txt" "$MAX_EPOCH_BUCKETS")
    if [ "$v" -eq -2 ]; then
        log "WARNING: ${PER_OBJECT_INFO}: epoch bucket capacity reached" \
            "(${MAX_EPOCH_BUCKETS}), skipping comparison to avoid false positives from bucket reuse"
        print_metric_values "$PER_OBJECT_INFO" "epoch"
    elif [ "$v" -eq -1 ]; then
        per_object_no_data_streak=$((per_object_no_data_streak + 1))
        log "${PER_OBJECT_INFO}: no common data across nodes" \
            "(streak: ${per_object_no_data_streak}/${NO_DATA_PATIENCE})"
        print_metric_values "$PER_OBJECT_INFO" "epoch"
        if [ "$per_object_no_data_streak" -ge "$NO_DATA_PATIENCE" ]; then
            die "${PER_OBJECT_INFO}: no common data for ${NO_DATA_PATIENCE} consecutive rounds — likely silent regression"
        fi
    elif [ "$v" -gt 0 ]; then
        log "INVARIANT VIOLATION — ${PER_OBJECT_INFO} (${v} epoch(s)):"
        cat "$WORK_DIR/details_per_object.txt"
        print_metric_values "$PER_OBJECT_INFO" "epoch"
        die "${PER_OBJECT_INFO}: mismatched digests across nodes"
    else
        per_object_no_data_streak=0
        log "${PER_OBJECT_INFO}: OK"
        print_metric_values "$PER_OBJECT_INFO" "epoch"
    fi

    # ------------------------------------------------------------------
    # Hard invariant 3: per-object pooled blob digest must match across nodes.
    # Same epoch bucket design as invariant 1; see comment above.
    # ------------------------------------------------------------------
    v=$(check_cross_node_metric \
        "$PER_OBJECT_POOLED_INFO" "epoch" \
        "$WORK_DIR/details_per_object_pooled.txt" "$MAX_EPOCH_BUCKETS")
    if [ "$v" -eq -2 ]; then
        log "WARNING: ${PER_OBJECT_POOLED_INFO}: epoch bucket capacity reached" \
            "(${MAX_EPOCH_BUCKETS}), skipping comparison to avoid false positives from bucket reuse"
        print_metric_values "$PER_OBJECT_POOLED_INFO" "epoch"
    elif [ "$v" -eq -1 ]; then
        per_object_pooled_no_data_streak=$((per_object_pooled_no_data_streak + 1))
        log "${PER_OBJECT_POOLED_INFO}: no common data across nodes" \
            "(streak: ${per_object_pooled_no_data_streak}/${NO_DATA_PATIENCE})"
        print_metric_values "$PER_OBJECT_POOLED_INFO" "epoch"
        if [ "$per_object_pooled_no_data_streak" -ge "$NO_DATA_PATIENCE" ]; then
            die "${PER_OBJECT_POOLED_INFO}: no common data for ${NO_DATA_PATIENCE} consecutive rounds — likely silent regression"
        fi
    elif [ "$v" -gt 0 ]; then
        log "INVARIANT VIOLATION — ${PER_OBJECT_POOLED_INFO} (${v} epoch(s)):"
        cat "$WORK_DIR/details_per_object_pooled.txt"
        print_metric_values "$PER_OBJECT_POOLED_INFO" "epoch"
        die "${PER_OBJECT_POOLED_INFO}: mismatched digests across nodes"
    else
        per_object_pooled_no_data_streak=0
        log "${PER_OBJECT_POOLED_INFO}: OK"
        print_metric_values "$PER_OBJECT_POOLED_INFO" "epoch"
    fi

    # ------------------------------------------------------------------
    # Hard invariant 4: event source must match across nodes.
    # The metric is recorded every NUM_EVENTS_PER_DIGEST_RECORDING events
    # and stored under `bucket = (event_index / N_PER_RECORDING) %
    # NUM_DIGEST_BUCKETS`. Within a single bucket generation, either a
    # node hasn't reached that batch (no data) or it has the final hash,
    # so common labels must match. We pass MAX_DIGEST_BUCKETS so that if
    # nodes ever drift far enough for a bucket to wrap, we stop comparing
    # rather than flag the stale collision as divergence.
    # ------------------------------------------------------------------
    v=$(check_cross_node_metric \
        "$EVENT_SOURCE" "bucket" \
        "$WORK_DIR/details_event_source.txt" "$MAX_DIGEST_BUCKETS")
    if [ "$v" -eq -2 ]; then
        log "WARNING: ${EVENT_SOURCE}: bucket capacity reached" \
            "(${MAX_DIGEST_BUCKETS}), skipping comparison to avoid false positives from bucket reuse"
        print_metric_values "$EVENT_SOURCE" "bucket"
    elif [ "$v" -eq -1 ]; then
        # No data yet — the metric is recorded every NUM_EVENTS_PER_DIGEST_RECORDING
        # events, which can take tens of minutes of cluster runtime. The observer
        # has no reliable way to know when "long enough" has elapsed (its own
        # lifetime resets on restart), so we just log absence. If this persists
        # for NO_DATA_PATIENCE rounds, treat it as a silent regression and die.
        event_source_no_data_streak=$((event_source_no_data_streak + 1))
        log "${EVENT_SOURCE}: no data yet" \
            "(streak: ${event_source_no_data_streak}/${NO_DATA_PATIENCE})"
        print_metric_values "$EVENT_SOURCE" "bucket"
        if [ "$event_source_no_data_streak" -ge "$NO_DATA_PATIENCE" ]; then
            die "${EVENT_SOURCE}: no common data for ${NO_DATA_PATIENCE} consecutive rounds — likely silent regression"
        fi
    elif [ "$v" -gt 0 ]; then
        log "INVARIANT VIOLATION — ${EVENT_SOURCE} (${v} bucket(s)):"
        cat "$WORK_DIR/details_event_source.txt"
        print_metric_values "$EVENT_SOURCE" "bucket"
        die "${EVENT_SOURCE}: mismatched values across nodes"
    else
        event_source_no_data_streak=0
        log "${EVENT_SOURCE}: OK"
        print_metric_values "$EVENT_SOURCE" "bucket"
    fi

    # ------------------------------------------------------------------
    # Soft invariant 1: all blobs fully stored (tolerate recovery lag).
    # A node that just recovered may briefly report < 1 while syncing
    # completes.  Crash only after FULLY_STORED_PATIENCE rounds.
    # Only the latest (highest) epoch bucket per node is checked — see
    # check_fully_stored_ratio for rationale.
    # ------------------------------------------------------------------
    v=$(check_fully_stored_ratio "$WORK_DIR/details_fully_stored.txt" "$MAX_EPOCH_BUCKETS")
    if [ "$v" -eq -2 ]; then
        log "WARNING: ${FULLY_STORED_RATIO}: epoch bucket capacity reached" \
            "(${MAX_EPOCH_BUCKETS}), skipping check — latest-epoch heuristic is unreliable after wrap"
        fully_stored_streak=0
        print_metric_values "$FULLY_STORED_RATIO" "epoch"
    elif [ "$v" -gt 0 ]; then
        fully_stored_streak=$((fully_stored_streak + 1))
        log "Fully-stored-ratio violation (streak: ${fully_stored_streak}/${FULLY_STORED_PATIENCE}):"
        cat "$WORK_DIR/details_fully_stored.txt"
        print_metric_values "$FULLY_STORED_RATIO" "epoch"
        if [ "$fully_stored_streak" -ge "$FULLY_STORED_PATIENCE" ]; then
            die "${FULLY_STORED_RATIO}: persistent violation for ${FULLY_STORED_PATIENCE} consecutive rounds"
        fi
    else
        fully_stored_streak=0
        log "${FULLY_STORED_RATIO}: OK"
        print_metric_values "$FULLY_STORED_RATIO" "epoch"
    fi

    log "All checks passed for round ${round}"
    sleep "$CHECK_INTERVAL"
done
