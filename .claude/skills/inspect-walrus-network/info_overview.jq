{
  epoch: .epochInfo.currentEpoch,
  epochStart: .epochInfo.startOfCurrentEpoch.DateTime,
  epochDurationDays: (.epochInfo.epochDuration.secs / 86400),
  maxEpochsAhead: .epochInfo.maxEpochsAhead,
  nNodes: .storageInfo.nNodes,
  nShards: .storageInfo.nShards,
  storagePriceFROST: .priceInfo.storagePricePerUnitSize,
  writePriceFROST: .priceInfo.writePricePerUnitSize,
  maxBlobSizeGB: (.sizeInfo.maxBlobSize / 1073741824 * 100 | floor / 100),
  nPrimarySourceSymbols: .committeeInfo.nPrimarySourceSymbols,
  nSecondarySourceSymbols: .committeeInfo.nSecondarySourceSymbols
}
