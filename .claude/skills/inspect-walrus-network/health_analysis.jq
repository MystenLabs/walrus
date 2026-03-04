def ok: .healthInfo.Ok;
def ep: ok.eventProgress;
def ss: ok.shardSummary.ownedShardStatus;
.healthInfo as $all |
($all | [.[] | select(ok) | ep.persisted] | max) as $maxEv |
($all | [.[] | select(ok) | ok.latestCheckpointSequenceNumber]
  | max) as $maxCp |
($all | [.[] | select(ok) | ok.epoch] | max) as $curEpoch |
{
  total: ($all | length),
  healthy: [$all[] | select(ok)] | length,
  unreachable: [$all[] | select(.healthInfo.Err)] | length,
  unreachableNodes: [
    $all[] | select(.healthInfo.Err) |
    {name: .nodeName, url: .nodeUrl, id: .nodeId}
  ],
  currentEpoch: $curEpoch,
  eventStats: {
    max: $maxEv,
    min: [$all[] | select(ok) | ep.persisted] | min
  },
  checkpointStats: {
    max: $maxCp,
    min: [$all[] | select(ok)
      | ok.latestCheckpointSequenceNumber] | min
  },
  epochMismatch: [
    $all[] | select(ok) | select(ok.epoch != $curEpoch) |
    {name: .nodeName, epoch: ok.epoch}
  ],
  shardsInRecovery: [
    $all[] | select(ok) | select(ss.inRecovery > 0) |
    {name: .nodeName, inRecovery: ss.inRecovery}
  ],
  shardsInTransfer: [
    $all[] | select(ok) | select(ss.inTransfer > 0) |
    {name: .nodeName, inTransfer: ss.inTransfer}
  ],
  lowUptime: [
    $all[] | select(ok) | select(ok.uptime.secs < 3600) |
    {name: .nodeName, uptimeSecs: ok.uptime.secs}
  ],
  eventLaggards: [
    $all[] | select(ok)
    | select(ep.persisted < ($maxEv - 100000)) |
    { name: .nodeName, persisted: ep.persisted,
      behind: ($maxEv - ep.persisted),
      pending: ep.pending }
  ] | sort_by(.behind) | reverse,
  checkpointLaggards: [
    $all[] | select(ok)
    | select(ok.latestCheckpointSequenceNumber
        < ($maxCp - 1000000)) |
    { name: .nodeName,
      checkpoint: ok.latestCheckpointSequenceNumber,
      lagBehind: ($maxCp
        - ok.latestCheckpointSequenceNumber) }
  ] | sort_by(.lagBehind) | reverse
}
