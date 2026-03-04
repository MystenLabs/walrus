# Usage: set NODE_NAMES as a jq variable, e.g.:
#   jq --argjson names '["Node A", "Node B"]' -f health_detail.jq
.healthInfo | [
  .[] |
  . as $node |
  select(any($names[]; . == $node.nodeName)) |
  { name: .nodeName,
    detail: .healthInfo.Ok | {
      epoch, uptime: .uptime.secs,
      persisted: .eventProgress.persisted,
      pending: .eventProgress.pending,
      checkpoint: .latestCheckpointSequenceNumber,
      shardStatus: .shardSummary,
      shardDetail: .shardDetail
    }
  }
]
